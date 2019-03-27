# FOP and UO web UI access

## General description

Develop system able to cache data about FOP's and UO's in Ukraine and
provide web-search for such data. Web search should be able to search
on available data and provide list with results and detailed
information about person. In case if many results can be for one request
pagination should be provided.

Use any technologies to cache data and speed up search. Use Docker
containers with required software solutions. Use Docker-Compose if
required. Use Makefiles if necessary.

Develop rich and complete documentation with information how to start
application and how to use it.

## Data source

Download data source from https://data.gov.ua from `Finances` datasets.
Load dataset for FOP's and UO's which is zip archive with `.xml` data.

Direct link (may be invalid after update):
https://data.gov.ua/dataset/1c7f3815-3259-45e0-bdf1-64dca07ddc10

Archive with data should be approx. 400 MiB. Decompressed data divided
into two `.xml` files each of them approx. 1 GiB.
