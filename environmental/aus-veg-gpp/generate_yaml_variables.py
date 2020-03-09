import json
import oyaml as yaml  # ordered yaml

class GenerateYamlVariables:

    def __init__(self):
        self.guide = {}  # data to be loaded
        self.results = []  # transformed results
        self._load_guide()

    def _load_guide(self):
        """Loads a template json file as a guide to the whole operation."""
        with open("meta.guide4.json", "r") as fp:
            self.guide = json.load(fp)


    def _transform(self):
        """Transforms layer names only"""
        count = 0
        for col in self.guide["data"]["collections"]:
            for ds in col["datasets"]:
                for layer in ds["layers"]:
                    print("Processing ... {}".format(layer["filename"]))
                    count += 1
                    item = {
                        "id": self._make_id(layer["filename"]),  # makes id from filename
                        "type": "Concept",
                        "prefLabel": {
                            "en": layer["title"].strip()
                        },
                        "measure_type": "continuous",
                        "units": layer["unit"],
                        "description": layer["tooltip"].strip()
                    }
                    self.results.append(item)
        print("Processed {} items.".format(count))

    def _make_id(self, value):
        base, _ = value.rsplit(".", 1)
        id = base.lower().replace(".", "_").replace("(", "").replace(")", "")
        return id

    def run(self):
        """Runs this program"""
        self._transform()
        self._write_results('new_variables.yaml')

    def _write_results(self, fpath):
        """Outputs the results to a yaml file."""
        with open(fpath, 'w') as fout:
            yaml.dump(self.results, fout)


if __name__ == '__main__':
    app = GenerateYamlVariables()
    app.run()
