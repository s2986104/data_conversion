import json
import uuid
import copy


class MetadataGenerator:

    COL_IDX_IN_GUIDE = 1  # collection index in guide.json

    def __init__(self):
        self.guide = {}  # input json data serving as a guide
        self.collection = {}  # output json data, goes to collections.json
        self.datasets = []  # output json data, goes to datasets.json
        self.data = []  # output json data, goes to data.json
        self.destination = "source/aus-enviro-topography"  # where to write output files

    def _load_guide(self):
        """Loads a template json file as a guide to the whole operation."""
        with open("meta.guide3.json", "r") as fp:
            self.guide = json.load(fp)

    def _generate_collection(self):
        """Generates the collection.json file based on the meta.guide.json
        This program works on generating metadata files for the first collection only.
        """

        collection_guide = self.guide["data"]["collections"][self.COL_IDX_IN_GUIDE]
        self.collection = {
          "type": "CollectionList",
          "collections": [
            {
              "_id": "aus-enviro-topography",
              "type": "Collection",
              "uuid": str(uuid.uuid4()),  # collection uuid
              "title": collection_guide["collection_name"],
              "description": collection_guide["collection_description"],
              "categories": [
                "climate"
              ],
              "rights": [
                {
                  "type": "License",
                  "value": collection_guide["datasets"][0]["licence"]
                }
              ],
              "landingPage": collection_guide["datasets"][0]["landingpage"],
              "attributions": [
                {
                  "type": "Citation",
                  "value": collection_guide["datasets"][0]["citation"]
                }
              ],
              "subjects": [
                  "Topography indices identifying areas of high flat areas or deposited material in flat valley bottoms in Australia."
              ]
            }
          ]
        }
        collection_path = "{}/collection.json".format(self.destination)
        self._write_results(collection_path, self.collection)

    @staticmethod
    def _write_results(fpath, data):
        """Outputs the results to a json file."""
        with open(fpath, 'w') as fout:
            json.dump(data, fout, indent=2)

    def _generate_datasets(self):
        """Generates the datasets.json file based on the meta.guide.json """
        collection_guide = self.guide["data"]["collections"][self.COL_IDX_IN_GUIDE]
        guide_datasets = collection_guide["datasets"]

        for in_ds in guide_datasets:
            self._generate_dataset(collection_guide, in_ds)  # builds self.datasets

        datasets_path = "{}/datasets.json".format(self.destination)
        self._write_results(datasets_path, self.datasets)

    def _generate_dataset(self, collection_guide, in_dataset):
        ds = {
            "type": "Coverage",
            "title": in_dataset["title"],
            "description": in_dataset["description"],
            "domain": {
              "type": "Domain",
              "domainType": in_dataset["domain"],
              "axes": {
                "x": {
                  "start": 112.9995833309998,
                  "stop": 153.9995833342801,
                  "num": 49200
                },
                "y": {
                  "start": -44.0004166673,
                  "stop": -10.000416664580001,
                  "num": 40800
                }
              },
              "referencing": [
                {
                  "coordinates": [
                    "x",
                    "y"
                  ],
                  "system": {
                    "type": "GeographicCRS",
                    "id": "http://www.opengis.net/def/crs/EPSG/0/4326",
                    "wkt": "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4326\"]]"
                  }
                }
              ]
            },
            "parameters": {},  # inserted by code
            "ranges": {},
            "rangeAlternates": {},  # inserted by code
            "bccvl:metadata": {
                "uuid": str(uuid.uuid4()),  # dataset uuid
                "categories": [
                    collection_guide["collection_type"],
                    collection_guide["collection_subtype"]
                ],
                "description_full": in_dataset["description_full"],
                "citation": in_dataset["citation"],
                "citation-url": in_dataset["citation-url"],
                "provider": in_dataset["provider"],
                "landingpage": in_dataset["landingpage"],
                "domain": in_dataset["domain"],
                "spatial_domain": "Australia",
                "time_domain": in_dataset["period"],
                "resolution": in_dataset["resolution"],
                "acknowledgement": in_dataset["provider"],
                "external_url": in_dataset["doi"],
                "license": in_dataset["licence"],
                "title": in_dataset["title"],
                "year": in_dataset["published"],
                "year_range": in_dataset["year_range"],
                "extent_wgs84": {
                    "bottom": -44.0004166673,
                    "left": 112.9995833309998,
                    "top": -10.000416664580001,
                    "right": 153.9995833342801
                },
                "partof": [
                    self.collection["collections"][0]["uuid"]
                ]
            }
          }
        ds["parameters"] = self._collect_parameters(in_dataset)
        ds["rangeAlternates"] = self._collect_alternates(in_dataset)

        self.datasets.append(ds)

    def _collect_parameters(self, in_dataset):
        parameters = {}
        for f in in_dataset["layers"]:
            base, _ = f["filename"].split(".")
            parametername = f["parametername"]
            parameters[parametername] = {
              "type": "Parameter",
              "observedProperty": {
                "label": {
                  "en": f["title"]
                },
                "dmgr:statistics": f["info"]["stats"],
                "dmgr:nodata": f["meta"]["nodata"],
                "dmgr:legend": f["unitfull"]
              },
              "tooltip": f["tooltip"],
              "unit": {
                "symbol": {
                  "value": f["unit"],
                  "type": f["unitfull"]
                }
              }
            }
        return parameters

    def _collect_alternates(self, in_dataset):
        alternates = {}
        tiffs = {}
        for f in in_dataset["layers"]:
            base_filename, _ = f["filename"].split(".")
            parametername = f["parametername"]
            tiffs[parametername] = {
                "type": "dmgr:TIFF2DArray",
                "datatype": "uint8",
                "axisNames": [
                    "y",
                    "x"
                ],
                "shape": f["info"]["shape"],
                "dmgr:band": 1,
                "dmgr:offset": f["meta"]["transform"][2],
                "dmgr:scale": f["meta"]["transform"][0],
                "dmgr:missingValue": f["meta"]["nodata"],
                "dmgr:min": f["info"]["stats"][0]["min"],
                "dmgr:max": f["info"]["stats"][0]["max"],
                "dmgr:datatype": f["meta"]["dtype"],  # "Float64",
                "url": self._get_url(in_dataset, base_filename)
            }
        alternates["dmgr:tiff"] = tiffs
        return alternates

    @staticmethod
    def _get_url(in_dataset, base_filename):
        url_base = "https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677"
        (_, _, collection_name, dataset_name) = in_dataset["folder_location"].split("/")
        return "{}/aus-csiro_layers/{}/{}/{}.tif".format(url_base, collection_name, dataset_name, base_filename)

    def _generate_data(self):
        """Generates data.json from info in self.datasets
        For each file in the dataset.parameters section,
        copy the dataset section,
             with one corresponding item in parameters section, and,
             one corresponding item in rangeAlternates.tiff section
        """
        for ds in self.datasets:
            file_names = list(ds["parameters"].keys())
            for f in file_names:
                new_item = copy.deepcopy(ds)
                new_item["parameters"] = {f: ds["parameters"][f]}  # copies one file item only
                new_item["rangeAlternates"]["dmgr:tiff"] = {f: ds["rangeAlternates"]["dmgr:tiff"][f]}  # copies one item
                new_item["bccvl:metadata"]["url"] = ds["rangeAlternates"]["dmgr:tiff"][f]["url"]  # copies url
                new_item["bccvl:metadata"]["uuid"] = str(uuid.uuid4())  # layer uuid
                del new_item["bccvl:metadata"]["partof"]
                self.data.append(new_item)

        datafile_path = "{}/data.json".format(self.destination)
        self._write_results(datafile_path, self.data)

    @staticmethod
    def _flatten(file_list):
        return [item for sublist in file_list for item in sublist]

    def run(self):
        self._load_guide()
        self._generate_collection()
        self._generate_datasets()
        self._generate_data()


if __name__ == '__main__':
    app = MetadataGenerator()
    app.run()
