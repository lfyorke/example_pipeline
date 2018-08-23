import json
import collections

class TestPipeline:
    """
    Primary class to control flow of data in pipeline
    """

    def __init__(self, source_file_path):
        self.source_file_path = source_file_path


    def flatten(self, d, parent_key='', sep='_'):
        """

        :param d:
        :param parent_key:
        :param sep:
        :return:
        """
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def clean_imp(self, data):
        """

        :param data:
        :return:
        """
        if 'imp' in data:
            data['imp'] = data['imp'][0]
        return data


    def del_features(self, data, features=['imp_banner_battr']):
        """

        :param data:
        :param features:
        :return:
        """
        for feat in features:
            if feat in data:
                del data[feat]
        return data


    def remove_lists(self, data):
        """

        :param data:
        :return:
        """

        for key, value in data.items():
            if isinstance(value, list):
                data[key] = value[0]
        return data

    def extract(self, source_file_path):
        """
        Method to read dataset from input filepath.

        :param file_path:
        :return:
        """
        with open(source_file_path) as f:
            for line in f:
                string = line.strip()
                d = json.loads(string)
                yield d

    def transform(self):
        """
        Method to apply transformations to the data.

        :param data:
        :return:
        """
        pass

    def load(self, f, data):
        """
        Method to write transformed dataset.

        :param data:
        :return:
        """
        f.write(str(data) + "\n")

    def run(self):
        """
        Convenience method to run pipeline.

        :return:
        """
        with open('results.txt', 'a') as f:
            for row in self.extract(self.source_file_path):
                flattened = self.flatten(row)
                cleaned = self.remove_lists(self.flatten(self.clean_imp(flattened)))
                feature_dict = self.del_features(cleaned)
                self.load(f, feature_dict)

                print(feature_dict)
                self.transform()

if __name__ == "__main__":
    pipeline = TestPipeline(source_file_path='part-00000-tid-8372074912937359139-bed8e65e-b634-4e1b-9e14-af856a68bdd0-10-c000.txt')
    pipeline.run()
