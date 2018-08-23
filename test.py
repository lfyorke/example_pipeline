

class TestPipeline:
    """
    Primary class to control flow of data in pipeline
    """

    def __init__(self, source_file_path):
        self.source_file_path = source_file_path


    def extract(self):
        """
        Method to read dataset from input filepath.

        :param file_path:
        :return:
        """
        pass

    def transform(self):
        """
        Method to apply transformations to the data.

        :param data:
        :return:
        """
        pass

    def load(self):
        """
        Method to write transformed dataset.

        :param data:
        :return:
        """
        pass

    def run(self):
        """
        Convenience method to run pipeline.

        :return:
        """

        self.extract()
        self.load()
        self.transform()

if __name__ == "__main__":
    pipeline = TestPipeline(source_file_path='part-00000-tid-8372074912937359139-bed8e65e-b634-4e1b-9e14-af856a68bdd0-10-c000.txt')
    pipeline.run()
