from vcis.aoi_classification.geolocation_data import GeoLocationData

class GeoLocationDataList:
    def __init__(self):
        self.geolocation_data_list = []

    def add_geolocation_data(self, geolocation_data: GeoLocationData):
        self.geolocation_data_list.append(geolocation_data)

    def get_geolocation_data(self, device_id_geo):
        for geolocation_data in self.geolocation_data_list:
            if geolocation_data.device_id_geo == device_id_geo:
                return geolocation_data

        return None

    def __getitem__(self, index):
        return self.geolocation_data_list[index]
    
    def get_index_by_device_id(self, device_id_geo):
        for i, data in enumerate(self.geolocation_data_list):
            if data.device_id_geo == device_id_geo:
                return i
        return None
    
    def get_length(self):
        return len(self.geolocation_data_list)
