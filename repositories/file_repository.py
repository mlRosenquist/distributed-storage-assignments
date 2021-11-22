import datetime

from mongoengine import connect
import bson

from models.file import File

connectionString = "mongodb://127.0.0.1:27017/files"
connect(host=connectionString)


def get_files():
    files =  File.objects
    return list(files)


def get_rs_files():
    files = File.objects(storage_mode__exact='erasure_coding_rs')
    return list(files)


def get_rlnc_files():
    files = File.objects(storage_mode__exact='erasure_coding_rlnc')
    return list(files)


def get_file(id: str):
    file = File.objects.get(id=bson.objectid.ObjectId(id))
    return file


def add_file(file: File):
    return file.save()


def remove_file_by_id(id: str):
    file = get_file(id)
    return file.delete()


def remove_file(file: File):
    return file.delete()


