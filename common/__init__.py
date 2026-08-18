"""Shared Pinterest Ads API + Glue/Iceberg helpers, used by every job in jobs/.

Packaged separately from the job scripts so it can be zipped and deployed to
each Glue job via --extra-py-files (see ../README.md for how). Nothing in
here is Glue-job-specific except glue_args.py.
"""
