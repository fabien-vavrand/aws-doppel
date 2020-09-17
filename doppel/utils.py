import os
import zipfile
import pathlib


def get_root_path():
    return str(pathlib.Path(__file__).parent.parent)


def zip_dir(path):
    import io
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip:
        for root, dirs, files in os.walk(path):
            for file in files:
                zip.write(os.path.join(root, file), arcname=os.path.relpath(os.path.join(root, file), path))
    return zip_buffer


def zip_dir_to(path, to_path):
    zip = zip_dir(path)
    with open(to_path, 'wb') as zip_file:
        zip_file.write(zip.getvalue())
