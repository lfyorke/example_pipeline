import json
import collections
import pickle

class TestPipeline:
    """
    Primary class to control flow of data in pipeline
    """

    def __init__(self, source_file_path):
        self.source_file_path = source_file_path


    def flatten(self, d, parent_key='', sep='_'):
        """
        Method to flatten nested dicitonary.

        :param d:  {dict} single line of data
        :param parent_key: {str} the parent key of a nested dictionary
        :param sep: {str} separator to be used for concatenated keys
        :return: {dict} single line of data
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
        Method to clean 'imp' feature, removing it from a list and applying self.flatten method.

        :param data:  {dict} single line of data
        :return:  {dict} single line of data
        """
        if 'imp' in data:
            data['imp'] = data['imp'][0]
        return data


    def del_features(self, data, features=['imp_banner_battr']):
        """
        This method deletes unwanted features from data set.

        :param data:  {dict} single line of data
        :param features: {list} list of strings, noting features names to be dropped
        :return:  {dict} single line of data
        """
        for feat in features:
            if feat in data:
                del data[feat]
        return data


    def remove_lists(self, data):
        """
        Method to remove unnecessary lists from input data structure.

        :param data:  {dict} single line of data
        :return:  {dict} single line of data
        """

        for key, value in data.items():
            if isinstance(value, list):
                data[key] = value[0]
        return data

    def extract(self, source_file_path):
        """
        Method to read dataset from input filepath.

        :param file_path: {str} representing the file path of input data
        :return:  {dict} single line of data, iterable
        """
        with open(source_file_path) as f:
            for line in f:
                string = line.strip()
                d = json.loads(string)
                yield d

    def transform(self, data, row_num):
        """
        Transform data into desired output format

        :param data: {dict} single line of data
        :return:  {dict} single line of data
        """
        data = (row_num, data)
        return data

    def load(self, f, data):
        """
        Method to write transformed dataset to a plain .txt file.

        :param data: {dict} single line of data
        """
        f.write(str(data) + "\n")

    def run(self):
        """
        Convenience method to run pipeline.
        """
        count = 0
        with open('results.txt', 'a') as f:
            for row in self.extract(self.source_file_path):
                count += 1
                flattened = self.flatten(row)
                cleaned = self.remove_lists(self.flatten(self.clean_imp(flattened)))
                feature_dict = self.del_features(cleaned)
                vector = self.transform(feature_dict, count)
                self.load(f, vector)
                if count % 1000 == 0:
                    print(count)


if __name__ == "__main__":
    pipeline = TestPipeline(source_file_path='part-00000-tid-8372074912937359139-bed8e65e-b634-4e1b-9e14-af856a68bdd0-10-c000.txt')
    with open('pipeline.p', 'wb') as f:
        pickle.dump(pipeline, f)
    pipeline.run()
