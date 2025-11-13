from extract.job.carte_pmr_job import CartePmrJob
from extract.job.ref_gare_idf_job import RefGareIdfJob
from extract.job.etablissement_job import EtablissementJob


def run():
    CartePmrJob().process()
    RefGareIdfJob().process()
    EtablissementJob().process()
