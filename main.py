import os.path
import sys, getopt
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pathlib import Path
import hashlib
import io
import datetime
from tqdm import tqdm
from googleapiclient.http import MediaIoBaseDownload

outputDirectory = "./output"
service = None
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.metadata']
folders = []


class Folder:
    def __init__(self, id, parent, name):
        self.id = id
        self.parent = parent
        self.name = name
        self.path = None


def parse_args(argv):
    global outputDirectory
    try:
        opts, args = getopt.getopt(argv, "o:", [])
    except getopt.GetoptError:
        print("Invalid arguments", file=sys.stderr)
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-o":
            outputDirectory = arg


def connect():
    global service
    global SCOPES
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('drive', 'v3', credentials=creds)
    except HttpError as error:
        print(f'An error occurred: {error}', file=sys.stderr)


def get_folder_list():
    try:
        page_token = None
        while True:
            # pylint: disable=maybe-no-member
            response = service.files().list(
                q="(mimeType = 'application/vnd.google-apps.folder') and ('me' in owners)",
                includeItemsFromAllDrives=False,
                spaces='drive', fields='nextPageToken, files(driveId, id, name, parents)',
                pageToken=page_token).execute()
            for item in response.get('files', []):
                folders.append(Folder(item.get("id"), item.get("parents")[0], item.get("name")))

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
    except HttpError as error:
        print(f'An error occurred: {error}', file=sys.stderr)


def get_root_folders():
    global service
    global folders
    try:
        # pylint: disable=maybe-no-member
        response = service.files().list(
            q="(not mimeType = 'application/vnd.google-apps.folder') and ('root' in parents)",
            includeItemsFromAllDrives=False,
            spaces='drive', fields='nextPageToken, files(driveId, id, name, parents)').execute()
        files = response.get('files', [])
        folders.append(Folder(files[0].get('parents')[0], "", "."))
    except HttpError as error:
        print(f'An error occurred: {error}', file=sys.stderr)


def calculate_paths():
    global folders
    folderMap = {}
    for f in folders:
        folderMap[f.id] = f
    for f in folders:
        if f.parent == "":
            f.path = f.name
        else:
            current = f
            total = ""
            while current.path is None:
                total = current.name + "/" + total
                current = folderMap[current.parent]
            f.path = current.path + "/" + total[:-1]


def make_folders_in_fs():
    global folders
    for f in folders:
        Path(outputDirectory + "/" + f.path).mkdir(parents=True, exist_ok=True)


def downloadFiles():
    global service
    global folders
    i = 0
    for f in tqdm(folders):
        files = []
        try:
            page_token = None
            while True:
                response = service.files().list(
                    q="(not mimeType = 'application/vnd.google-apps.folder') and ('" + f.id + "' in parents)",
                    includeItemsFromAllDrives=False,
                    spaces='drive',
                    fields='nextPageToken, files(driveId, id, name, parents,mimeType, md5Checksum, modifiedTime)',
                    pageToken=page_token).execute()
                files.extend(response.get('files', []))
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break
        except HttpError as error:
            print(f'An error occurred: {error}')
        for i in files:
            downloadFile(i, f)


def downloadFile(file, folder):
    global service
    filename = file.get("name")
    path = outputDirectory + "/" + folder.path + "/" + filename
    mime = file.get("mimeType")
    if "application/vnd.google-apps" in mime:
        download_app_file(file, folder)
        return
    file_id = file.get("id")
    if os.path.exists(path):
        if md5(path) == file.get("md5Checksum"):
            # print("Skipping existing file " + path)
            return
        os.remove(path)
    try:
        # pylint: disable=maybe-no-member
        request = service.files().get_media(fileId=file_id)
        filecontent = io.BytesIO()
        downloader = MediaIoBaseDownload(filecontent, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(F'Download {path} {int(status.progress() * 100)}.')
        with open(path, "wb") as outfile:
            # Copy the BytesIO stream to the output file
            outfile.write(filecontent.getbuffer())
    except HttpError as error:
        print(F'An error occurred: {error}', file=sys.stderr)


def convert_and_download(file, target_path, target_mime):
    global service
    if os.path.exists(target_path):
        modified = file.get("modifiedTime")
        fs_modified = datetime.datetime.utcfromtimestamp(os.path.getmtime(target_path))
        if datetime.datetime.fromisoformat(modified[:-1]) < fs_modified:
            # print("Skipping existing file: " + target_path)
            return

    file_id = file.get("id")
    try:
        # pylint: disable=maybe-no-member
        request = service.files().export_media(fileId=file_id, mimeType=target_mime)
        filecontent = io.BytesIO()
        downloader = MediaIoBaseDownload(filecontent, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(F'Download {target_path} {int(status.progress() * 100)}.')
        with open(target_path, "wb") as outfile:
            # Copy the BytesIO stream to the output file
            outfile.write(filecontent.getbuffer())
    except HttpError as error:
        print(F'An error occurred: {error}', file=sys.stderr)


def download_app_file(file, folder):
    mime = file.get("mimeType")
    filename = file.get("name")
    path = outputDirectory + "/" + folder.path + "/" + filename
    if mime == "application/vnd.google-apps.jam":
        # print("skipping JAM file: " + path)
        return
    if mime == "application/vnd.google-apps.shortcut":
        # print("skipping shortcut file: " + path)
        return
    if mime == "application/vnd.google-apps.spreadsheet":
        convert_and_download(file, path + ".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        return
    if mime == "application/vnd.google-apps.document":
        convert_and_download(file, path + ".docx",
                             "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        return
    if mime == "application/vnd.google-apps.presentation":
        convert_and_download(file, path + ".pptx",
                             "application/vnd.openxmlformats-officedocument.presentationml.presentation")
        return
    if mime == "application/vnd.google-apps.drawing":
        convert_and_download(file, path + ".png", "image/png")
        return
    print("Skipping " + path + ". Unknown mime type: " + mime)


# https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def md5(file):
    hash = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash.hexdigest()


def main(argv):
    print("Validating command")
    parse_args(argv)
    print("Connecting to drive service")
    connect()
    print("Getting drive structure")
    get_root_folders()
    get_folder_list()
    print("Analysing drive structure")
    calculate_paths()
    print("Creating folders in output directory")
    make_folders_in_fs()
    print("Downloading files")
    downloadFiles()


if __name__ == '__main__':
    main(sys.argv[1:])
