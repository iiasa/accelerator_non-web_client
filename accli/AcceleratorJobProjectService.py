import os
import io
import sys
import typing
import requests
import json
import base64
import urllib3
from urllib.parse import quote
from pathlib import Path
from typing import List, Tuple

from accli.common import todict

class AccAPIError(Exception):
    pass

    # def __init__(self, *args, **kwargs):
    #     self.status_code = kwargs.pop('status_code')
    #     self.response_data = kwargs.pop('response_data')
    #     super().__init__(*args, **kwargs)

class UnhealthyControlServerError(Exception):
    pass

retries = urllib3.util.Retry(total=10, backoff_factor=1)

http_client = urllib3.poolmanager.PoolManager(
    num_pools=20, retries=retries
)

http_client_wo_cert_verification = urllib3.poolmanager.PoolManager(
    cert_reqs="CERT_NONE", num_pools=20, retries=retries
)

class AcceleratorJobProjectService:
    def __init__(
            self,
            user_token, 
            server_url='http://accelerator-api.iiasa.ac.at',
            verify_cert=True
        ):
        
        self.user_token = user_token

        if verify_cert:
            self.http_client = http_client
        else:
            self.http_client = http_client_wo_cert_verification

        self.cli_base_url = f"{server_url}/v1/ajob-cli"
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
            )
        return res

    def get_file_stat(self, bucket_object_id):
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/file-stat/{bucket_object_id}",
            headers=self.common_request_headers
        )
        return todict(res.data)
    
    
    def enumerate_files_by_prefix(self, prefix):
        project_slug = filename.split('/')[0]


        b64_encoded_prefix = base64.b64encode(prefix.encode()).decode()

        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/{project_slug}/enumerate-all-files/{b64_encoded_prefix}",
            headers=self.common_request_headers
        )
        if res.data:
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
    
    def get_dataset_type(self, *args, **kwargs):
        return self.get_bucket_object_validation_type(*args, **kwargs)

    def get_bucket_object_validation_type(self, bucket_object_id):
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/dataset-type/{bucket_object_id}",
            headers=self.common_request_headers
        )
        if res.data:
            return todict(res.data)


    def get_filename_dataset_type(self, filename):
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/filename-dataset-type/?filename={filename}",
            headers=self.common_request_headers
        )
        if res.data:
            return todict(res.data)

    def get_filename_validation_details(self, filename):
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/filename-validation-detail/?filename={filename}",
            headers=self.common_request_headers
        )
        if res.data:
            return todict(res.data)

    def get_bucket_object_validation_details(self, bucket_object_id):
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/validation-detail/{bucket_object_id}",
            headers=self.common_request_headers
        )
        if res.data:
            return todict(res.data)

    
    def get_file_url(self, bucket_object_id):
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/file-url/{bucket_object_id}",
            headers=self.common_request_headers
        )

        if res.data:
            return todict(res.data)


    def get_file_stream(self, bucket_object_id):
        url = self.get_file_url(bucket_object_id)
        if url:
            resp = self.http_client_request("GET", url, preload_content=False)
            return resp

    def check_job_health(self):
        try:
            res = self.http_client_request(
                "GET",
                f"{self.cli_base_url}/is-healthy/",
                headers=self.common_request_headers
            )
            if res.data:
                res = todict(res.data)
        except Exception as err:
            raise UnhealthyControlServerError(str(err))
        
        is_healthy = res['is_healthy']
        return is_healthy
        
    def add_log_file(self, data: bytes, filename):
        try:
            res = self.http_client_request(
                "GET",
                f"{self.cli_base_url}/presigned-log-upload-url/?filename={filename}",
                headers=self.common_request_headers
            )
            if res.data:
                res = todict(res.data)
        except Exception as err:
            raise UnhealthyControlServerError(str(err))

        upload_url = res['upload_url']
        app_bucket_id = res['app_bucket_id']
        res_filename = res['filename']
        is_healthy = res['is_healthy']

        requests.put(
            upload_url,
            data=data,
            # headers=headers,
            verify=False,
        )

        try:
            self.http_client_request(
                "POST",
                f"{self.cli_base_url}/register-log-file/",
                json=dict(
                    filename=res_filename,
                    app_bucket_id=app_bucket_id
                ),
                headers=self.common_request_headers
            )
        except Exception as err:
            raise UnhealthyControlServerError(str(err))

        if not is_healthy:
            raise UnhealthyControlServerError('Unhealthy control server.')

        

    def get_multipart_put_create_signed_url(
        self,
        app_bucket_id,
        object_name,
        upload_id,
        part_number
    ):
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/put-create-signed-url",
            fields=dict(
                app_bucket_id=app_bucket_id,
                object_name=object_name,
                upload_id=upload_id,
                part_number=part_number
            ),
            headers=self.common_request_headers
        )

        return todict(res.data)

    def get_multipart_put_update_signed_url(
        self,
        filename,
        upload_id,
        part_number,
    ):
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/put-update-signed-url",
            fields=dict(
                filename=filename,
                upload_id=upload_id,
                part_number=part_number
            ),
            headers=self.common_request_headers
        )

        return todict(res.data)

    def get_put_create_multipart_upload_id(self, filename):
        encoded_filename = quote(filename)
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/multipart-upload-id/?filename={encoded_filename}",
            headers=self.common_request_headers
        )

        data = todict(res.data)

        return data['upload_id'], data['app_bucket_id'], data['uniqified_filename']


    def get_validator_create_multipart_upload_id(self, filename):
        encoded_filename = quote(filename)
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/validator-multipart-upload-id/?filename={encoded_filename}",
            headers=self.common_request_headers
        )

        data = todict(res.data)

        return data['upload_id'], data['app_bucket_id'], data['uniqified_filename']

    def get_put_update_multipart_upload_id(self, filename):
        encoded_filename = quote(filename)
        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/update-multipart-upload-id/?filename={encoded_filename}",
            headers=self.common_request_headers
        )

        return todict(res.data)

    def complete_job_multipart_upload(
        self,
        app_bucket_id,
        filename,
        upload_id,
        parts: List[Tuple[str, str]],
        is_log_file=False
    ):
        headers = {"Content-Type": "application/json"}

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "PUT", 
            f"{self.cli_base_url}/complete-create-multipart-upload",
            json=dict(
                app_bucket_id=app_bucket_id,
                filename=filename,
                upload_id=upload_id,
                parts=base64.b64encode(json.dumps(parts).encode()).decode(),
                is_log_file=is_log_file
            ),
            headers=headers
        )

        return todict(res.data)


    def complete_validator_multipart_upload(
        self,
        app_bucket_id,
        filename,
        upload_id,
        parts: List[Tuple[str, str]]
    ):
        headers = {"Content-Type": "application/json"}

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "PUT", 
            f"{self.cli_base_url}/complete-validator-multipart-upload",
            json=dict(
                app_bucket_id=app_bucket_id,
                filename=filename,
                upload_id=upload_id,
                parts=base64.b64encode(json.dumps(parts).encode()).decode()
            ),
            headers=headers
        )

        return todict(res.data)

    def complete_update_multipart_upload(
        self, filename, upload_id, parts: List[Tuple[str, str]]
    ):

        headers = {"Content-Type": "application/json"}

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "PUT", 
            f"{self.cli_base_url}/complete-update-multipart-upload",
            json=dict(
                filename=filename,
                upload_id=upload_id,
                parts=base64.b64encode(json.dumps(parts).encode()).decode()
            ),
            headers=headers
        )

        return todict(res.data)

    def abort_create_multipart_upload(self, app_bucket_id, filename, upload_id):
        
        headers = {"Content-Type": "application/json"}

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "PUT", 
            f"{self.cli_base_url}/abort-create-multipart-upload",
            json=dict(
                app_bucket_id=app_bucket_id,
                filename=filename,
                upload_id=upload_id
            ),
            headers=headers
        )

    def abort_update_multipart_upload(self, filename, upload_id):
        
        headers = {
            # "Content-Type": "application/json"
        }

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "PUT", 
            f"{self.cli_base_url}/abort-update-multipart-upload",
            json=dict(
                filename=filename,
                upload_id=upload_id
            ),
            headers=headers
        )
   
    # # TODO @wrufesh ensure it requires special token
    # def register_iamc_validation(
    #     self, validated_bucket_object_id, indexdb_bucket_object_id
    # ):

    #     headers = {"Content-Type": "application/json"}

    #     headers.update(self.common_request_headers)

    #     res = self.http_client_request(
    #         "PUT", 
    #         f"{self.cli_base_url}/register-iamc-validation",
    #         json=dict(
    #             validated_bucket_object_id=validated_bucket_object_id,
    #             indexdb_bucket_object_id=indexdb_bucket_object_id
    #         ),
    #         headers=headers
    #     )

    def register_validation(
        self, 
        validated_bucket_object_id: int,
        dataset_template_id: int,
        validated_metadata: dict,
        validation_supporting_bucket_object_ids: List[int]
    ):
        headers = {"Content-Type": "application/json"}

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "PUT", 
            f"{self.cli_base_url}/register-validation",
            json=dict(
                validated_bucket_object_id=validated_bucket_object_id,
                dataset_template_id=dataset_template_id,
                validated_metadata=validated_metadata,
                validation_supporting_bucket_object_ids=validation_supporting_bucket_object_ids
            ),
            headers=headers
        )

    def get_dataset_template_details(
        self, 
        dataset_template_id
    ):
        headers = {"Content-Type": "application/json"}

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "GET", 
            f"{self.cli_base_url}/dataset-template-detail/{dataset_template_id}",
            headers=headers
        )

        return todict(res.data)

      

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

    def add_filestream_as_job_output(self, filename, file_stream, is_log_file=False):
        headers = dict()
        headers["Content-Type"] = "application/octet-stream"

        part_size, part_count = 50 * 1024**2, -1

        upload_id = None
        app_bucket_id = None
        uniqified_filename = None

        one_byte = b""
        stop = False
        part_number = 0
        parts = []
        uploaded_size = 0
        put_presigned_url = None

        try:
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
                        filename, 
                        # headers=headers
                    )

                put_presigned_url = self.get_multipart_put_create_signed_url(
                    app_bucket_id, uniqified_filename, upload_id, part_number
                )

                part_upload_response = requests.put(
                    put_presigned_url,
                    data=part_data,
                    # headers=headers,
                    verify=False,
                )

                etag = part_upload_response.headers.get("etag").replace('"', "")
                parts.append((part_number, etag))

            created_bucket_object_id = self.complete_job_multipart_upload(
                app_bucket_id, uniqified_filename, upload_id, parts, is_log_file=is_log_file
            )
            return created_bucket_object_id

        except Exception as err:
            # Cancel if any error
            if upload_id:
                self.abort_create_multipart_upload(
                    app_bucket_id,
                    uniqified_filename,
                    upload_id,
                )
            raise err

    def replace_bucket_object_id_content(self, filename, file_stream):
        headers = dict()
        headers["Content-Type"] = "application/octet-stream"

        part_size, part_count = 50 * 1024**2, -1

        upload_id = None
        app_bucket_id = None
        uniqified_filename = None

        one_byte = b""
        stop = False
        part_number = 0
        parts = []
        uploaded_size = 0
        put_presigned_url = None

        try:
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
                    upload_id = self.get_put_update_multipart_upload_id(
                        filename
                    )

                put_presigned_url = self.get_multipart_put_update_signed_url(
                    filename, upload_id, part_number
                )

                part_upload_response = requests.put(
                    put_presigned_url,
                    data=part_data,
                    # headers=headers,
                    verify=False,
                )

                etag = part_upload_response.headers.get("etag").replace('"', "")
                parts.append((part_number, etag))

            created_bucket_object_id = self.complete_update_multipart_upload(
                filename,
                upload_id,
                parts,
            )

            return created_bucket_object_id
        except Exception as err:
            # Cancel if any error
            if upload_id:
                self.abort_update_multipart_upload(
                    filename,
                    upload_id,
                )

            raise err


    def add_filestream_as_validation_supporter(self, filename, file_stream, is_log_file=False):
        headers = dict()
        headers["Content-Type"] = "application/octet-stream"

        part_size, part_count = 50 * 1024**2, -1

        upload_id = None
        app_bucket_id = None
        uniqified_filename = None

        one_byte = b""
        stop = False
        part_number = 0
        parts = []
        uploaded_size = 0
        put_presigned_url = None

        try:
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
                    ) = self.get_validator_create_multipart_upload_id(
                        filename, 
                        # headers=headers
                    )

                put_presigned_url = self.get_multipart_put_create_signed_url(
                    app_bucket_id, uniqified_filename, upload_id, part_number
                )

                part_upload_response = requests.put(
                    put_presigned_url,
                    data=part_data,
                    # headers=headers,
                    verify=False,
                )

                etag = part_upload_response.headers.get("etag").replace('"', "")
                parts.append((part_number, etag))

            created_bucket_object_id = self.complete_validator_multipart_upload(
                app_bucket_id, uniqified_filename, upload_id, parts
            )
            return created_bucket_object_id

        except Exception as err:
            # Cancel if any error
            if upload_id:
                self.abort_create_multipart_upload(
                    app_bucket_id,
                    uniqified_filename,
                    upload_id,
                )
            raise err

    def update_job_status(self, status):
        headers = {
            # "Content-Type": "application/json"
            # 'Content-Type': 'application/x-www-form-urlencoded'
        }

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "POST", 
            f"{self.cli_base_url}/webhook-event",
            json=dict(
                type='STATUS_UPDATE',
                data=dict(
                    new_status=status
                )
            ),
            headers=headers
        )

class Fs:
    @staticmethod
    def write_stream_remote(filestream, dest_filepath, user_token, server_url):

        accelerator_job_service = AcceleratorJobProjectService(
            user_token, 
            server_url=server_url,
            verify_cert=True
        )

        accelerator_job_service.add_filestream_as_job_output(
            dest_filepath,
            filestream
        )

    @staticmethod
    def write_stream_local(filestream, dest_filepath):
        with open(dest_filepath, 'wb') as file:
            file.write(filestream.getvalue())

    @staticmethod
    def get_file_url(remote_filepath):
        user_token = os.environ.get("ACC_JOB_TOKEN", None)
        server_url = os.environ.get("ACC_JOB_GATEWAY_SERVER", None)

        if not (user_token and server_url):
            raise ValueError("Remote data repository credentials not found.")
        
        accelerator_job_service = AcceleratorJobProjectService(
            user_token, 
            server_url=server_url,
            verify_cert=True
        )

        return accelerator_job_service.get_file_url_from_repo(
            remote_filepath
        )
        

    @staticmethod
    def write_file(source: typing.Union[str, io.BytesIO], dest_filepath):
        user_token = os.environ.get("ACC_JOB_TOKEN", None)
        server_url = os.environ.get("ACC_JOB_GATEWAY_SERVER", None)

        if isinstance(source, io.BytesIO):
            if (user_token and server_url):
                Fs.write_stream_remote(source, dest_filepath, user_token, server_url)
            else:
                Fs.write_stream_local(source, dest_filepath)
        else:
            if type(source) != str:
                raise ValueError("Unknown source argument")
            source_path = Path(source)
            if not source_path.exists():
                raise ValueError("Source path does not exist")

            with open(source, 'rb') as fstream:
                if (user_token and server_url):
                    Fs.write_stream_remote(fstream, dest_filepath, user_token, server_url)
                else:
                    Fs.write_stream_local(fstream, dest_filepath)

    @staticmethod
    def enumerate_files_by_prefix(prefix):
        user_token = os.environ.get("ACC_JOB_TOKEN", None)
        server_url = os.environ.get("ACC_JOB_GATEWAY_SERVER", None)

        if not (user_token and server_url):
            raise ValueError("Remote data repository credentials not found.")
        
        accelerator_job_service = AcceleratorJobProjectService(
            user_token, 
            server_url=server_url,
            verify_cert=True
        )

        return accelerator_job_service.enumerate_files_by_prefix(
            prefix
        )
