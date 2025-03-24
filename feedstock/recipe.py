
import earthaccess 
import s3fs
import xarray as xr 
from distributed import Client 
auth = earthaccess.login(strategy="interactive", persist=True)

client = Client(n_workers=16)

search_results = earthaccess.search_data(
    short_name="TELLUS_GRAC_L3_JPL_RL06_LND_v04", 
    provider="POCLOUD",
)
fs = earthaccess.get_fsspec_https_session()

netcdf_urls = [fs.open(result.data_links()[0]) for result in search_results]

cds = xr.open_mfdataset(netcdf_urls, 
                        parallel=True, engine='h5netcdf',coords="minimal", data_vars="minimal", compat='override')




fs = s3fs.S3FileSystem(
    key="", secret="", client_kwargs={"endpoint_url": "https://nyu1.osn.mghpcc.org"}
)

mapper = fs.get_mapper("leap-pangeo-pipeline/GRACE-GRACE-FO-TWS/GRACE-GRACE-FO-TWS.zarr")

cds.chunk({'time':200,'lat':180,'lon':360}).to_zarr(
    mapper, mode="w", consolidated=True, zarr_format=2
)
