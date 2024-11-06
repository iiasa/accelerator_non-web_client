import os
import requests
import urllib3
import json
import base64
import concurrent.futures
from typing import List, Tuple
from rich.progress import Progress

from accli.common import todict

class AccAPIError(Exception):

    def __init__(self, message, response, status_code):            
            
            super().__init__(message)
                
            self.response = response
            self.status_code = status_code

retries = urllib3.util.Retry(total=10, backoff_factor=1)

http_client = urllib3.poolmanager.PoolManager(
    num_pools=20, retries=retries
)

http_client_wo_cert_verification = urllib3.poolmanager.PoolManager(
    cert_reqs="CERT_NONE", num_pools=20, retries=retries
)

class AcceleratorTerminalCliProjectService:
    def __init__(
            self,
            user_token, 
            server_url,
            verify_cert=True
        ):
        
        self.user_token = user_token

        if verify_cert:
            self.http_client = http_client
        else:
            self.http_client = http_client_wo_cert_verification

        self.server_url = server_url
        self.cli_base_url = f"{self.server_url}/v1/aterm-cli"
        self.common_request_headers = {
            'x-authorization': user_token
        }

    def http_client_request(self, *args, **kwargs):

        if urllib3.__version__.startswith('1.'):
            if 'json' in kwargs:
                json_dict = kwargs.pop('json')

                assert isinstance(json_dict, dict), (
                    f"urllib request: json keyword argument should be dict."
                )

                encoded_data = json.dumps(json_dict).encode('utf-8')

                kwargs['body'] = encoded_data

        res = self.http_client.request(*args, **kwargs)

        if str(res.status)[0] in ['4', '5']:
            raise AccAPIError(
                f"Accelerator api error:: status_code={res.status} :: response_data={res.data}", 
                status_code=res.status, 
                response=res
            )
        
        return res

    def get_file_stat(self, project_slug, filename):
        
        try:
            res = self.http_client_request(
                "POST", 
                f"{self.cli_base_url}/{project_slug}/file-stat/",
                json=dict(filename=filename),
                headers=self.common_request_headers
            )
        except AccAPIError as err:
            if err.status_code == 404:
                return None
            else:
                raise err

        return todict(res.data)
    
    def get_file_url_from_repo(self, filename):
        project_slug = filename.split('/')[0]
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/{project_slug}/get-file-download-url/?filename={filename}",
            headers=self.common_request_headers
        )
        if res.data:
            return todict(res.data)
    
    def get_github_app_token(self, project_slug):
        
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/{project_slug}/github-app-token/",
            headers=self.common_request_headers
        )

        return todict(res.data)
    
    def get_jobstore_push_url(self, project_slug, filename):
        
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/{project_slug}/jobstore-push-url/?filename={filename}",
            headers=self.common_request_headers
        )

        if res.status == 409 or res.status == '409':
            return None

        return todict(res.data)
    
    def dispatch(self, project_slug, job_description):
        
        try:
            res = self.http_client_request(
                "POST", 
                f"{self.cli_base_url}/{project_slug}/jobs/dispatch/",
                json=job_description,
                headers=self.common_request_headers
            )
        except AccAPIError as err:
                raise err

        return todict(res.data)['job_id']
    
    def get_dataset_template_details(self, project_slug, template_slug):
        
        try:
            res = self.http_client_request(
                "GET", 
                f"{self.server_url}/v1/projects/{project_slug}/dataset-templates/{template_slug}/by-slug/",
            )
        except AccAPIError as err:
            if err.status_code == 404:
                return None
            else:
                raise err

        return todict(res.data)

    
    def get_multipart_put_create_signed_url(
        self,
        project_slug,
        app_bucket_id,
        object_name,
        upload_id,
        part_number
    ):
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/{project_slug}/put-multipart-signed-url",
            fields=dict(
                app_bucket_id=app_bucket_id,
                object_name=object_name,
                upload_id=upload_id,
                part_number=part_number
            ),
            headers=self.common_request_headers
        )

        return todict(res.data)

    

    def get_put_create_multipart_upload_id(self, project_slug, filename):
        
        b64_filename = base64.b64encode(filename.encode()).decode()

        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/{project_slug}/create-multipart-upload-id/{b64_filename}",
            headers=self.common_request_headers
        )

        data = todict(res.data)

        return data['upload_id'], data['app_bucket_id'], data['uniqified_filename']


    def complete_create_multipart_upload(
        self,
        project_slug,
        app_bucket_id,
        filename,
        upload_id,
        parts: List[Tuple[str, str]]
    ):
        headers = {"Content-Type": "application/json"}

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "PUT", 
            f"{self.cli_base_url}/{project_slug}/complete-create-multipart-upload",
            json=dict(
                app_bucket_id=app_bucket_id,
                filename=filename,
                upload_id=upload_id,
                parts=base64.b64encode(json.dumps(parts).encode()).decode()
            ),
            headers=headers
        )

        return todict(res.data)

    
    def abort_create_multipart_upload(self, project_slug, app_bucket_id, filename, upload_id):
        
        headers = {"Content-Type": "application/json"}

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "PUT", 
            f"{self.cli_base_url}/{project_slug}/abort-create-multipart-upload",
            json=dict(
                app_bucket_id=app_bucket_id,
                filename=filename,
                upload_id=upload_id
            ),
            headers=headers
        )

    

    def read_part_data(self, stream, size, part_data=b"", progress=None):
        """Read part data of given size from stream."""
        size -= len(part_data)
        while size:
            data = stream.read(size)
            if not data:
                break  # EOF reached
            if not isinstance(data, bytes):
                raise ValueError("read() must return 'bytes' object")
            part_data += data
            size -= len(data)
            if progress:
                progress.update(len(data))
        return part_data
    

    def put_part(self, project_slug, app_bucket_id, uniqified_filename, upload_id, part_number, part_data, progress, task):

        put_presigned_url = self.get_multipart_put_create_signed_url(
                        project_slug, app_bucket_id, uniqified_filename, upload_id, part_number
                    )
        
        part_upload_response = requests.put(
            put_presigned_url,
            data=part_data,
            # headers=headers,
            verify=False,
        )

        progress.update(task, advance=len(part_data))

        etag = part_upload_response.headers.get("etag").replace('"', "")
        return part_number, etag


    def upload_filestream_to_accelerator(self, project_slug, filename, file_stream, progress, task, max_workers=os.cpu_count()):
        headers = dict()
        headers["Content-Type"] = "application/octet-stream"

        part_size, part_count = 200 * 1024**2, -1

        upload_id = None
        app_bucket_id = None
        uniqified_filename = None

        one_byte = b""
        stop = False
        part_number = 0
        parts = []
        uploaded_size = 0
        put_presigned_url = None

        futures = []

        try:

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

                while not stop:
                    part_number += 1
                    part_data = self.read_part_data(
                        file_stream,
                        part_size + 1,
                        one_byte,
                        progress=None,
                    )

                    # If part_data_size is less or equal to part_size,
                    # then we have reached last part.
                    if len(part_data) <= part_size:
                        part_count = part_number
                        stop = True
                    else:
                        one_byte = part_data[-1:]
                        part_data = part_data[:-1]

                    uploaded_size += len(part_data)

                    if not upload_id:
                        (
                            upload_id,
                            app_bucket_id,
                            uniqified_filename,
                        ) = self.get_put_create_multipart_upload_id(
                            project_slug,
                            filename, 
                            # headers=headers
                        )

                        
                    future = executor.submit(self.put_part, project_slug, app_bucket_id, uniqified_filename, upload_id, part_number, part_data, progress, task)

                    futures.append(future)
            
            futures_results = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

            parts = [f.result() for f in futures_results.done]

            parts.sort(key=lambda x: x[0])


            created_bucket_object_id = self.complete_create_multipart_upload(
                project_slug, app_bucket_id, uniqified_filename, upload_id, parts
            )
            return created_bucket_object_id

        except Exception as err:
            # Cancel if any error
            if upload_id:
                self.abort_create_multipart_upload(
                    project_slug,
                    app_bucket_id,
                    uniqified_filename,
                    upload_id,
                )

            raise err

    