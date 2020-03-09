import json
import uuid
import copy

class MetadataGenerator:

    COL_IDX_IN_GUIDE = 0  # collection index in guide.json
    def __init__(self):
        self.guide = {}  # input json data serving as a guide
        self.collection = {}  # output json data, goes to collections.json
        self.datasets = []  # output json data, goes to datasets.json
        self.data = []  # output json data, goes to data.json
        self.destination = "source/aus-clim-csiro"  # where to output results

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
              "_id": "aus-clim-csiro",
              "type": "Collection",
              "uuid": str(uuid.uuid4()),
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
                "Climate variables for continental Australia"
              ]
            }
          ]
        }
        collection_path = "{}/collection.json".format(self.destination)
        self._write_results(collection_path, self.collection)

    def _write_results(self, fpath, data):
        """Outputs the results to a json file."""
        with open(fpath, 'w') as fout:
            json.dump(data, fout, indent=2)

    def _generate_datasets(self):
        """Generates the datasets.json file based on the meta.guide.json """
        collection_guide = self.guide["data"]["collections"][0]
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
            "description_full": in_dataset["description_full"],
            "domain": {
              "type": "Domain",
              "domainType": in_dataset["domain"],
              "axes": {
                "x": {
                  "start": 112.9,
                  "stop": 154.0,
                  "num": 16440
                },
                "y": {
                  "start": -43.7425,
                  "stop": -8.0,
                  "num": 14297
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
                    "id": "http://www.opengis.net/def/crs/EPSG/0/4151",
                    "wkt": "GEOGCS[\"GRS 1980(IUGG, 1980)\",DATUM[\"unknown\",SPHEROID[\"GRS80\",6378137,298.257222101],TOWGS84[0,0,0,0,0,0,0]],PRIMEM[\"Greenwich\",0],UNIT[\"degree\",0.0174532925199433]]"
                  }
                }
              ]
            },
            "parameters": {},  # inserted by code
            "ranges": {},
            "rangeAlternates": {},  # inserted by code
            "bccvl:metadata": {
              "categories": [
                collection_guide["collection_type"],
                collection_guide["collection_subtype"]
              ],
              "domain": in_dataset["domain"],
              "spatial_domain": "Australia",
              "time_domain": in_dataset["period"],
              "resolution": in_dataset["resolution"],
              "acknowledgement": in_dataset["provider"],
              "external_url": in_dataset["doi"],
              "license": in_dataset["licence"],
              "title": in_dataset["title"],
              "year": in_dataset["published"],
              "year_range": [
                1976,
                2005
              ],
              "extent_wgs84": {
                "bottom": -43.7425,
                "left": 112.9,
                "top": -8.0,
                "right": 154.0
              },
              "uuid": str(uuid.uuid4()),
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
                "datatype": "float",
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

    def _get_url(self, in_dataset, base_filename):
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
                self.data.append(new_item)

        datafile_path = "{}/data.json".format(self.destination)
        self._write_results(datafile_path, self.data)


    def _flatten(self, file_list):
        """Flattens a nested list."""
        return [item for sublist in file_list for item in sublist]

    def run(self):
        self._load_guide()
        self._generate_collection()
        self._generate_datasets()
        self._generate_data()


if __name__ == '__main__':
    app = MetadataGenerator()
    app.run()
