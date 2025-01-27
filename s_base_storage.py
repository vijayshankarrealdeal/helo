def create_bucket(s_base, bucket_name):
    response = s_base.storage.create_bucket(
        bucket_name,
        options={
            "public": False,
            "allowed_mime_types": ["image/png"],
            "file_size_limit": 1024,
        },
    )
    return response


def upload_file(
    file_path,
    s_base,
    bucket_name,
):
    file_name = file_path.split("/")[-1]
    with open(file_path, "rb") as f:
        response = s_base.storage.from_(bucket_name).upload(
            file=f,
            path=f"public/{file_name}",
            file_options={"cache-control": "3600", "upsert": "false"},
        )
    return response

def download_file():
    pass