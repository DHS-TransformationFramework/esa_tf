import os

from fastapi.responses import StreamingResponse


def loadDefinition():
    def iterfile():
        with open(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "metadata.xml"),
            mode="rb",
        ) as file_like:
            yield from file_like

    return StreamingResponse(iterfile(), media_type="text/xml")
