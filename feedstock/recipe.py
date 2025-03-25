import s3fs
import xarray as xr 
import os
from distributed import Client 

client = Client(n_workers=16)

#need to see how to mount google drive directory

def generate_mswep_filenames(start_year=1979, end_year=None):
    if end_year is None:
        end_year = datetime.datetime.now().year
    today = datetime.datetime.now()

    filenames = []
    for year in range(start_year, end_year + 1):
        # Determine number of days in the year
        is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
        n_days = 366 if is_leap else 365

        for day in range(1, n_days + 1):
            for hour in range(0, 24, 3):
                # Skip future dates in the current year
                date = datetime.datetime.strptime(f"{year}{day:03}", "%Y%j") + datetime.timedelta(hours=hour)
                if date > today:
                    continue

                filename = f"{year}{day:03}.{hour:02}.nc"
                filenames.append(filename)
    return filenames

# Generate the list and print the first 10 as an example
nc_filenames = generate_mswep_filenames()


fs = s3fs.S3FileSystem(
    key="", secret="", client_kwargs={"endpoint_url": "https://nyu1.osn.mghpcc.org"}
)

mapper = fs.get_mapper("leap-pangeo-pipeline/MSWEP/MSWEP.zarr")

cds.chunk({'time':200,'lat':180,'lon':360}).to_zarr(
    mapper, mode="w", consolidated=True, zarr_format=2
)
