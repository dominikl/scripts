import omero.scripts as scripts
import omero.util.script_utils as script_utils
from omero.gateway import BlitzGateway
import omero
from omero.rtypes import rstring, rlong
from pathlib import Path
import tempfile
import subprocess
import os
import zipfile
import shutil
from datetime import datetime


managed_repo_path = None
bf2raw_path = None


def get_path(conn, image_id):
    query = "select org from Image i left outer join i.fileset as fs left outer join fs.usedFiles as uf left outer join uf.originalFile as org where i.id = :iid"
    qs = conn.getQueryService()
    params = omero.sys.Parameters()
    params.map = {"iid": rlong(image_id)}
    results = qs.findAllByQuery(query, params)
    for res in results:
        name = res.name._val
        path = res.path._val
        if not (name.endswith(".log") or name.endswith(".txt") or name.endswith(".xml")):
            return path, name


def do_export(path, name, target_base):
    abs_path = Path(managed_repo_path) / path / name

    target = Path(target_base) / name
    target.mkdir(exist_ok=True)

    process = subprocess.Popen(["bin/bioformats2raw", "--file_type=zarr", abs_path, target],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               cwd=Path(bf2raw_path))
    stdout, stderr = process.communicate()
    if stderr:
        print(stderr)
    else:
        return target


def compress(target, base):
    """
    Creates a ZIP recursively from a given base directory.

    @param target:      Name of the zip file we want to write E.g.
                        "folder.zip"
    @param base:        Name of folder that we want to zip up E.g. "folder"
    """
    rootdir = os.path.basename(base)
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as zipObj:
        for folderName, subfolders, filenames in os.walk(base):
            for filename in filenames:
                filepath = os.path.join(folderName, filename)
                parentpath = os.path.relpath(filepath, base)
                arcname = os.path.join(rootdir, parentpath)
                zipObj.write(filepath, arcname)


def zarr_image_export(conn, script_params):
    message = ""
    objects, log_message = script_utils.get_objects(conn, script_params)
    message += log_message
    if not objects:
        return None, message
    if script_params["Data_Type"] == 'Dataset':
        images = []
        for ds in objects:
            images.extend(list(ds.listChildren()))
        if not images:
            message += "No image found in dataset(s)"
            return None, message
    else:
        images = objects

    tmp_dir = Path(tempfile.gettempdir())

    for img in images:
        print("Processing image: ID {}: {}".format(img.id, img.getName()))
        path, name = get_path(conn, img.id)
        exp_path = do_export(path, name, tmp_dir)
        if exp_path:
            zipfile_name = "{}.zip".format(name)
            zipfile_path = tmp_dir / zipfile_name
            compress(zipfile_path, exp_path)
            script_utils.create_link_file_annotation(
            conn, zipfile_path, img, output=zipfile_name, mimetype="application/zip")
            shutil.rmtree(exp_path)
            zipfile_path.unlink()


def run_script():
    data_types = [rstring('Dataset'), rstring('Image')]

    client = scripts.client(
        'Zarr_Image_Export.py',
        """Convert Images to Zarr and attach them as File annotation.""",

        scripts.String(
            "Data_Type", optional=False, grouping="1",
            description="The data you want to work with.", values=data_types,
            default="Image"),

        scripts.List(
            "IDs", optional=False, grouping="2",
            description="List of IDs").ofType(rlong(0)),

        version="0.0.1",
        authors=["Dominik Lindner", "OME Team"],
        institutions=["University of Dundee"],
        contact="ome-users@lists.openmicroscopy.org.uk",
    )

    try:
        start_time = datetime.now()

        conn = BlitzGateway(client_obj=client)

        global managed_repo_path
        global bf2raw_path
        cs = conn.getConfigService()
        managed_repo_path = cs.getConfigValue('omero.managed.dir')
        bf2raw_path = cs.getConfigValue('omero.bf2raw.dir')

        script_params = client.getInputs(unwrap=True)

        zarr_image_export(conn, script_params)

        stop_time = datetime.now()
        print("Duration: {}".format((stop_time - start_time)))

    finally:
        client.closeSession()


if __name__ == "__main__":
    run_script()
