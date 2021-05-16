from datetime import date
from io import BytesIO, StringIO
from tempfile import NamedTemporaryFile
from time import sleep, perf_counter
from typing import Any, Dict, Iterator, List, Optional, Tuple

import googleapiclient.discovery
import typer

from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account as ServiceAccount
from pydantic import BaseModel, BaseSettings
from pytube import YouTube as YoutubeDownloader
from rich.console import Console


class Settings(BaseSettings):
    GOOGLE_CREDENTIALS: str
    GOOGLE_AUDIO_PARENT_ID: str
    GOOGLE_REPORTING_PARENT_ID: str
    YOUTUBE_API_KEY: str
    GOOGLE_SCOPES: List[str] = ["https://www.googleapis.com/auth/drive"]
    YOUTUBE_PLAYLIST_ID: str = "PL6ULlZ_0mjaiYXY_oadeDfBiVnrNIHiLp"


def GoogleDrive(settings: Settings) -> googleapiclient.discovery.Resource:
    with NamedTemporaryFile("w") as credentials:
        credentials.write(settings.GOOGLE_CREDENTIALS)
        credentials.flush()
        return googleapiclient.discovery.build(
            "drive",
            "v3",
            credentials=ServiceAccount.Credentials.from_service_account_file(
                credentials.name,
                scopes=settings.GOOGLE_SCOPES,
            ),
        )


def Youtube(settings: Settings) -> googleapiclient.discovery.Resource:
    return googleapiclient.discovery.build(
        "youtube",
        "v3",
        developerKey=settings.YOUTUBE_API_KEY,
    )


def YoutubePlaylist(
    youtube: googleapiclient.discovery.Resource,
    playlist_id: str,
) -> Iterator[Tuple[str, str]]:
    has_next = True
    page_token = None
    while has_next:
        playlist = (
            youtube.playlistItems()
            .list(
                part="snippet,contentDetails",
                maxResults=25,
                playlistId=playlist_id,
                pageToken=page_token,
            )
            .execute()
        )
        for item in playlist.get("items", []):
            video = item.get("contentDetails", {}).get("videoId")
            title = item.get("snippet", {}).get("title")
            yield (video, title)
        page_token = playlist.get("nextPageToken")
        if not page_token:
            has_next = False


class PlaylistItem(BaseModel):
    video: str
    title: str


class Playlist(BaseModel):
    items: List[PlaylistItem] = []
    name: Optional[str]


def entrypoint() -> None:
    console = Console()
    settings = Settings()
    today = date.today()
    youtube = Youtube(settings)
    playlist = Playlist()
    console.print(":inbox_tray: downloading playlist metadata")
    for video, title in YoutubePlaylist(youtube, settings.YOUTUBE_PLAYLIST_ID):
        playlist.items.append(PlaylistItem(video=video, title=title))
    # NOTE: export to GDrive
    console.print(":inbox_tray: uploading playlist metadata to GDrive")
    drive = GoogleDrive(settings)
    drive.files().create(
        body={
            "name": f"youtube-{settings.YOUTUBE_PLAYLIST_ID}-{today:%Y}{today:%m}{today:%d}.json",
            "parents": [settings.GOOGLE_REPORTING_PARENT_ID],
        },
        media_body=MediaIoBaseUpload(
            StringIO(playlist.json()), mimetype="application/json"
        ),
    ).execute()
    # NOTE: download audio and export to GDrive
    for item in playlist.items:
        console.print(f":cd: downloading {item.title} from YouTube")
        try:
            buffer = BytesIO()
            (
                YoutubeDownloader(f"https://youtube.com/watch?v={item.video}")
                .streams.filter(only_audio=True)
                .first()
                .stream_to_buffer(buffer)
            )
            start = perf_counter()
            console.print(f":speak_no_evil: uploading {item.title}.mp3 to GDrive")
            drive.files().create(
                body={
                    "name": f"{item.title}.mp3",
                    "parents": [settings.GOOGLE_AUDIO_PARENT_ID],
                },
                media_body=MediaIoBaseUpload(buffer, mimetype="audio/mpeg3"),
            ).execute()
            delay = perf_counter() - start
            sleep(min(30 - delay, 0))
        except Exception as e:
            console.print(
                f":warning: [yellow]error while processing track {item.title}: {e}[/yellow]"
            )


# def download(id):
#    credentials = "service.account.json"
#    drive = get_drive(credentials)
#    request = drive.files().get_media(fileId=id)
#    with open("test_download.json", "wb") as stream:
#        downloader = MediaIoBaseDownload(stream, request)
#        eof = False
#        while not eof:
#            status, eof = downloader.next_chunk()


if __name__ == "__main__":
    typer.run(entrypoint)
