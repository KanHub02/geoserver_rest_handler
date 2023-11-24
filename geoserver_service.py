import requests
import os
import zipfile
from abc import ABC, abstractmethod

from decouple import config


class IGeoServerRequest(ABC):
    @abstractmethod
    def create_workspace(self, workspace_name: str):
        pass

    @abstractmethod
    def create_datastore(self, workspace_name: str, store_name: str, shapefile_path: str):
        pass

    @abstractmethod
    def update_shapefile(self, workspace_name: str, store_name: str, shapefile_path: str):
        pass

    @abstractmethod
    def get_shapefile(self, workspace_name: str, store_name: str, shapefile_path: str):
        pass

    @abstractmethod
    def publish_layer(self, workspace_name: str, store_name: str, layer_name: str):
        pass

    @abstractmethod
    def execute(self, workspace_name: str, store_name: str, shapefile_path: str, layer_name: str):
        pass


class IZipFileManager(ABC):
    pass


class GeoServerRequest(IGeoServerRequest):

        def __init__(self):
            self.geoserver_url = f"{config('URL_GEOSERVER')}geoserver/rest"
            self.username = config('USERNAME_GEOSERVER')
            self.password = config('PASSWORD_GEOSERVER')

        def create_workspace(self, workspace_name: str):
            workspace_url = f'{self.geoserver_url}/workspaces/{workspace_name}.json'
            workspace_exists = requests.get(workspace_url, auth=(self.username, self.password)).ok
            print(workspace_exists)

            if not workspace_exists:
                requests.post(f'{self.geoserver_url}/workspaces.json',
                              auth=(self.username, self.password),
                              headers={'Content-Type': 'application/json'},
                              json={'workspace': {'name': workspace_name}})

        def create_datastore(self, workspace_name: str, store_name: str, shapefile_path: str):
            store_url = f'{self.geoserver_url}/workspaces/{workspace_name}/datastores/{store_name}.json'
            store_exists = requests.get(store_url, auth=(self.username, self.password)).ok

            if not store_exists:
                requests.post(f'{self.geoserver_url}/workspaces/{workspace_name}/datastores.json',
                              auth=(self.username, self.password),
                              headers={'Content-Type': 'application/json'},
                              json={'dataStore': {
                                  'name': store_name,
                                  'type': 'Shapefile',
                                  'enabled': True,
                                  'connectionParameters': {
                                      'url': f'file:data_dir/workspaces/{workspace_name}/{shapefile_path}',
                                      'create spatial index': True
                                  }
                              }})

        def update_shapefile(self, workspace_name: str, store_name: str, shapefile_path: str):
            for shpfile in os.listdir(shapefile_path):
                if shpfile.endswith('.shp'):
                    shpfile_path_full = os.path.join(shapefile_path, shpfile)
                    with open(shpfile_path_full, 'rb') as f:
                        requests.put(
                            f'{self.geoserver_url}/workspaces/{workspace_name}/datastores/{store_name}/file.shp?update=overwrite',
                            auth=(self.username, self.password),
                            data=f
                            )

        def get_shapefile(self, workspace_name, store_name, layer_name, destination_path):
            geoserver_url = f"{config('URL_GEOSERVER')}geoserver"
            output_format = "shape-zip"
            download_url = f"{geoserver_url}/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace_name}:{layer_name}&outputFormat={output_format}"
            print(download_url)
            try:
                response = requests.get(download_url, auth=(self.username, self.password), stream=True)
                print(response.status_code)

                if response.status_code == 200:
                    os.makedirs(destination_path, exist_ok=True)
                    shapefile_zip_path = os.path.join(destination_path, f"{store_name}.zip")

                    with open(shapefile_zip_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=128):
                            f.write(chunk)

                    print(f"Shapefile saved to {shapefile_zip_path}")

                    with zipfile.ZipFile(shapefile_zip_path, 'r') as zip_ref:
                        zip_ref.extractall(destination_path)
                        print(f"Shapefile extracted to {destination_path}")

                else:
                    print(f"Failed to download shapefile: HTTP Status Code {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Error during request: {e}")

        def publish_layer(self, workspace_name: str, store_name: str, layer_name: str):
            layer_url = f'{self.geoserver_url}/workspaces/{workspace_name}/datastores/{store_name}/featuretypes'
            requests.post(layer_url,
                          auth=(self.username, self.password),
                          headers={'Content-Type': 'application/json'},
                          json={'featureType': {
                              'name': layer_name,
                              'nativeName': layer_name,
                              'title': layer_name,
                              'srs': 'EPSG:4326',
                              'enabled': True
                          }})

        def execute(self, workspace_name: str, store_name: str, shapefile_path: str, layer_name: str):
            # self.create_workspace(workspace_name)
            # self.create_datastore(workspace_name, store_name, shapefile_path)
            # self.update_shapefile(workspace_name, store_name, shapefile_path)
            # self.publish_layer(workspace_name, store_name, layer_name)
            destination_path = f"{shapefile_path}tmp"
            self.get_shapefile(workspace_name, store_name, layer_name, destination_path)