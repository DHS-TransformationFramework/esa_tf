import os


def loadDefinition():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "metadata.xml"),
        mode="rb",
    ) as file_like:
        yield from file_like
