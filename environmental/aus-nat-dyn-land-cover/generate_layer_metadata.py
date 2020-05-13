import json
import uuid
import copy
from typing import List, Dict, Tuple

class MetadataGenerator:

    COL_IDX_IN_GUIDE = 0  # collection index in guide.json

    def __init__(self):
        self.guide = {}        # input json data serving as a guide
        self.collection = {}   # output json data, goes to collections.json
        self.datasets = []     # output json data, goes to datasets.json
        self.data = []         # output json data, goes to data.json
        self.destination = "dest/national-dynamic-land-cover"  # where to write json files

    def _load_guide(self):
        """Loads a template json file as a guide to the whole operation."""
        with open("meta.guide5.json", "r") as fp:
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
              "_id": "national-dynamic-land-cover_layers",
              "type": "Collection",
              "uuid": str(uuid.uuid4()),
              "title": collection_guide["collection_name"],
              "description": collection_guide["collection_description"],
              "categories": [
                "environmental", "vegetation", "land cover"
              ],
              "rights": [
                {
                  "type": "License",
                  "value": collection_guide["licence"]
                }
              ],
              "landingPage": collection_guide["landingpage"],
              "attributions": [
                {
                  "type": "Citation",
                  "value": collection_guide["citation"]
                }
              ],
              "subjects": collection_guide["subjects"]
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
              "domainType": in_dataset["domainType"],
              "axes": {
                "x": {
                  "start": in_dataset["coords"]["x.left"],
                  "stop": in_dataset["coords"]["x.right"],
                  "num": in_dataset["width"]
                },
                "y": {
                  "start": in_dataset["coords"]["y.top"],
                  "stop": in_dataset["coords"]["y.bottom"],
                  "num": in_dataset["height"]
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
                    "wkt": "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AXIS[\"Latitude\",NORTH],AXIS[\"Longitude\",EAST],AUTHORITY[\"EPSG\",\"4326\"]]"
                  }
                }
              ]
            },
            "parameters": {},  # inserted by code
            "ranges": {},
            "rangeAlternates": {},  # inserted by code
            "bccvl:metadata": {
                "uuid": str(uuid.uuid4()),
                "title": in_dataset["title"],
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
                "spatial_domain": in_dataset["spatial_domain"],
                "time_domain": in_dataset["time_domain"],
                "resolution": in_dataset["resolution"],
                "acknowledgement": in_dataset["provider"],
                "external_url": in_dataset["doi"],
                "doi": in_dataset["doi"],
                "license": in_dataset["licence"],
                "year": in_dataset["published"],
                "year_range": in_dataset["year_range"],
                "extent_wgs84": {
                    "bottom": in_dataset["coords"]["y.bottom"],
                    "left": in_dataset["coords"]["x.left"],
                    "top":  in_dataset["coords"]["y.top"],
                    "right": in_dataset["coords"]["x.right"]
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
        for lyr in in_dataset["layers"]:
            base, _ = lyr["filename"].split(".")
            parametername = lyr["parametername"]
            parameters[parametername] = {
                "type": "Parameter",
                "observedProperty": {
                    "label": {
                      "en": lyr["title"]
                    },
                    "dmgr:statistics": lyr["info"]["stats"][0],
                    "dmgr:nodata": lyr["meta"]["nodata"],
                },
                "tooltip": lyr["tooltip"],
                "unit": lyr["unit"]
            }
            if "legend" in lyr:
                parameters[parametername]["observedProperty"]["dmgr:legend"] = lyr["legend"]

            if lyr["datatype"] == "categorical":
                parameters[parametername]["observedProperty"]["categories"] = lyr.get("categories")
                parameters[parametername]["categoryEncoding"] = lyr.get("categoryEncoding")

        return parameters

    def _collect_alternates(self, in_dataset):
        alternates = {}
        tiffs = {}
        for f in in_dataset["layers"]:
            base_filename, _ = f["filename"].split(".")
            parametername = f["parametername"]
            tiffs[parametername] = {
                "type": "dmgr:TIFF2DArray",
                # "datatype": f["meta"]["dtype"],
                "axisNames": [
                    "y",
                    "x"
                ],
                "shape": f["info"]["shape"],
                "dmgr:band": 1,
                "dmgr:offset": f["meta"]["transform"][1],
                "dmgr:scale": f["meta"]["transform"][8],
                "dmgr:missingValue": f["meta"]["nodata"],
                "dmgr:min": f["info"]["stats"][0]["min"],
                "dmgr:max": f["info"]["stats"][0]["max"],
                "dmgr:datatype": f["meta"]["dtype"],
                "url": f["url"]
            }
        alternates["dmgr:tiff"] = tiffs
        return alternates

    def _generate_data(self) -> None:
        """Generates data.json from info in self.datasets
        For each file in the dataset.parameters section,
        copy the dataset section,
             with one corresponding item in parameters section, and,
             one corresponding item in rangeAlternates.tiff section
        """
        for ds in self.datasets:
            parameters = list(ds["parameters"].keys())
            for f in parameters:
                new_item = copy.deepcopy(ds)
                new_item["parameters"] = {f: ds["parameters"][f]}  # copies one file item only
                new_item["rangeAlternates"]["dmgr:tiff"] = {f: ds["rangeAlternates"]["dmgr:tiff"][f]}  # copies one item
                new_item["bccvl:metadata"]["url"] = ds["rangeAlternates"]["dmgr:tiff"][f]["url"]  # copies url
                new_item["bccvl:metadata"]["uuid"] = str(uuid.uuid4())  # layer uuid
                new_item["bccvl:metadata"]["data_type"] = self._get_datatype_from_guide_layer(ds['title'], f)

                # any auxfiles?
                auxfiles = self._get_auxfiles_from_guide_layer(ds["title"], f)
                if auxfiles:  # not None
                    new_item["bccvl:metadata"]["auxfiles"] = auxfiles
                # else:
                #     new_item["bccvl:metadata"]["auxfiles"] = []

                del new_item["bccvl:metadata"]["partof"]
                del new_item["bccvl:metadata"]["categories"]
                self.data.append(new_item)


        datafile_path = "{}/data.json".format(self.destination)
        self._write_results(datafile_path, self.data)

    def _flatten(self, file_list) -> List:
        return [item for sublist in file_list for item in sublist]

    def run(self) -> None:
        self._load_guide()
        self._generate_collection()
        self._generate_datasets()
        self._generate_data()

    def _get_datatype_from_guide_layer(self, dataset_title: str, parameter_name: str) -> str:
        guide_ds = self._get_guide_dataset(dataset_title)
        guide_lyr = self._get_guide_layer(guide_ds, parameter_name)
        return guide_lyr.get("datatype", "continuous")  # default is "continuous"

    def _get_auxfiles_from_guide_layer(self, dataset_title: str, parameter_name: str) -> Dict:
        result = None
        guide_ds = self._get_guide_dataset(dataset_title)
        guide_lyr = self._get_guide_layer(guide_ds, parameter_name)
        result = guide_lyr.get("auxfiles", None)
        return result

    def _get_guide_layer(self, guide_ds: Dict, parameter_name: str) -> Dict:
        result = None
        for layer in guide_ds["layers"]:
            if layer["parametername"] == parameter_name:
                result = layer
                break
        return result

    def _get_guide_dataset(self, title: str) -> Dict:
        result = None
        collection_guide = self.guide["data"]["collections"][self.COL_IDX_IN_GUIDE]
        guide_datasets = collection_guide["datasets"]
        for ds in guide_datasets:
            if ds["title"] == title:
                result = ds
                break
        return result


if __name__ == '__main__':
    app = MetadataGenerator()
    app.run()
