"""Common Structs File"""
import collections


def stages_tuple():
    """Common Stages"""
    named_tuple = collections.namedtuple("named_tuple",
                                         ["ingest_driver", "ingest_controller", "ingest_worker",
                                          "ingest_parquet", "parquet_controller", "parquet_worker",
                                          "parquet_crawler"],
                                         rename=False)
    row = named_tuple(ingest_driver='Ingestion-Driver',
                      ingest_controller='Ingestion-Controller',
                      ingest_worker='Ingestion-Worker',
                      ingest_parquet="Ingest-Parquet",
                      parquet_controller="Parquet-Controller",
                      parquet_worker="Parquet-Worker",
                      parquet_crawler="Parquet-Crawler")
    return row


def status_tuple():
    """Common Statuses"""
    named_tuple = collections.namedtuple("named_tuple",
                                         ["pending", "in_progress", "completed", "failed"],
                                         rename=False)
    row = named_tuple(pending='pending',
                      in_progress='in progress',
                      completed='completed',
                      failed='failed')
    return row
