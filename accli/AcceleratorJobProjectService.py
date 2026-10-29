import os
import io
from typing import Union, List

import requests
import json
import base64
import urllib3
from pathlib import Path

from accli.common import todict

ACCLI_DEBUG = os.environ.get('ACCLI_DEBUG', False)


class AccAPIError(Exception):
    pass

    # def __init__(self, *args, **kwargs):
    #     self.status_code = kwargs.pop('status_code')
    #     self.response_data = kwargs.pop('response_data')
    #     super().__init__(*args, **kwargs)


retries = urllib3.util.Retry(total=10, backoff_factor=1)

http_client = urllib3.poolmanager.PoolManager(
    num_pools=20, retries=retries, maxsize=2000, block=True
)

http_client_wo_cert_verification = urllib3.poolmanager.PoolManager(
    cert_reqs="CERT_NONE", num_pools=20, retries=retries, maxsize=2000, block=True
)


class AcceleratorJobProjectService:
    def __init__(
            self,
            user_token,
            server_url='http://accelerator.iiasa.ac.at',
            verify_cert=(not ACCLI_DEBUG)
    ):

        self.user_token = user_token

        if verify_cert:
            self.http_client = http_client
        else:
            self.http_client = http_client_wo_cert_verification

        self.cli_base_url = f"{server_url}/api/v1/ajob-cli"
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
            f"{self.cli_base_url}/file-stat/{bucket_object_id}/",
            headers=self.common_request_headers
        )
        return todict(res.data)

    def enumerate_files_by_prefix(self, prefix):
        raise NotImplementedError("filename parameter is not implemented yet")

        project_slug = filename.split('/')[0]

        b64_encoded_prefix = base64.b64encode(prefix.encode()).decode()

        res = self.http_client_request(
            "GET",
            f"{self.cli_base_url}/{project_slug}/enumerate-all-files/{b64_encoded_prefix}/",
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
        return None

    def get_dataset_type(self, *args, **kwargs):
        return self.get_bucket_object_validation_type(*args, **kwargs)

    def get_bucket_object_validation_type(self, bucket_object_id):
        res = self.http_client_request(
            "GET",
            f"{self.cli_base_url}/dataset-type/{bucket_object_id}/",
            headers=self.common_request_headers
        )
        if res.data:
            return todict(res.data)
        return None

    def get_filename_dataset_type(self, filename):
        res = self.http_client_request(
            "GET",
            f"{self.cli_base_url}/filename-dataset-type/?filename={filename}",
            headers=self.common_request_headers
        )
        if res.data:
            return todict(res.data)
        return None

    def get_filename_validation_details(self, filename):
        res = self.http_client_request(
            "GET",
            f"{self.cli_base_url}/filename-validation-detail/?filename={filename}",
            headers=self.common_request_headers
        )
        if res.data:
            return todict(res.data)
        return None

    def get_bucket_object_validation_details(self, bucket_object_id):
        res = self.http_client_request(
            "GET",
            f"{self.cli_base_url}/validation-detail/{bucket_object_id}/",
            headers=self.common_request_headers
        )
        if res.data:
            return todict(res.data)
        return None

    def get_file_url(self, bucket_object_id):
        res = self.http_client_request(
            "GET",
            f"{self.cli_base_url}/file-url/{bucket_object_id}/",
            headers=self.common_request_headers
        )

        if res.data:
            return todict(res.data)
        return None

    def get_file_stream(self, bucket_object_id):
        url = self.get_file_url(bucket_object_id)
        if url:
            resp = self.http_client_request("GET", url, preload_content=False)
            return resp
        return None

    def check_job_health(self):

        res = self.http_client_request(
            "GET",
            f"{self.cli_base_url}/is-healthy/",
            headers=self.common_request_headers
        )

        if res.data:
            res = todict(res.data)

        is_healthy = res['is_healthy']
        return is_healthy

    def add_log_file(self, data: bytes, filename):

        res = self.http_client_request(
            "GET",
            f"{self.cli_base_url}/presigned-log-upload-url/?filename={filename}",
            headers=self.common_request_headers
        )

        if res.data:
            res = todict(res.data)
        else:
            raise ValueError(f"Unable to get presign url in response: {res.data}")

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

        self.http_client_request(
            "POST",
            f"{self.cli_base_url}/register-log-file/",
            json=dict(
                filename=res_filename,
                app_bucket_id=app_bucket_id
            ),
            headers=self.common_request_headers
        )

        return is_healthy

    # # TODO @wrufesh ensure it requires special token
    # def register_iamc_validation(
    #     self, validated_bucket_object_id, indexdb_bucket_object_id
    # ):

    #     headers = {"Content-Type": "application/json"}

    #     headers.update(self.common_request_headers)

    #     res = self.http_client_request(
    #         "PUT", 
    #         f"{self.cli_base_url}/register-iamc-validation/",
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
            f"{self.cli_base_url}/register-validation/",
            json=dict(
                validated_bucket_object_id=validated_bucket_object_id,
                dataset_template_id=dataset_template_id,
                validated_metadata=validated_metadata,
                validation_supporting_bucket_object_ids=validation_supporting_bucket_object_ids
            ),
            headers=headers
        )

    def register_validation_with_filename(
            self,
            validated_filename: str,
            dataset_template_id: int,
            validated_metadata: dict,
            validation_supporting_filenames: List[str]
    ):
        headers = {"Content-Type": "application/json"}

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "PUT",
            f"{self.cli_base_url}/register-validation-with-filename/",
            json=dict(
                validated_filename=validated_filename,
                dataset_template_id=dataset_template_id,
                validated_metadata=validated_metadata,
                validation_supporting_filenames=validation_supporting_filenames
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
            f"{self.cli_base_url}/dataset-template-detail/{dataset_template_id}/",
            headers=headers
        )

        return todict(res.data)

    def update_job_status(self, status):
        headers = {
            # "Content-Type": "application/json"
            # 'Content-Type': 'application/x-www-form-urlencoded'
        }

        headers.update(self.common_request_headers)

        res = self.http_client_request(
            "POST",
            f"{self.cli_base_url}/webhook-event/",
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
        raise NotImplementedError(
            "Remote uploads through accli are no longer supported. "
            "Write job outputs to /mnt/wdrv instead; the job agent uploads them."
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
            verify_cert=(not ACCLI_DEBUG)
        )

        return accelerator_job_service.get_file_url_from_repo(
            remote_filepath
        )

    @staticmethod
    def write_file(source: Union[str, io.BytesIO], dest_filepath):
        user_token = os.environ.get("ACC_JOB_TOKEN", None)
        server_url = os.environ.get("ACC_JOB_GATEWAY_SERVER", None)

        if isinstance(source, io.BytesIO):
            if user_token and server_url:
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
                if user_token and server_url:
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
            verify_cert=(not ACCLI_DEBUG)
        )

        return accelerator_job_service.enumerate_files_by_prefix(
            prefix
        )
