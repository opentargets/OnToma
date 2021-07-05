# ClinVar
wget -qO- 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/disease_names' \
  | tail -n+2 \
  | cut -f1 \
  | sort -uf \
  | shuf -n 100 \
> sample_clinvar.txt

# PhenoDigm
wget -qO- 'https://www.ebi.ac.uk/mi/impc/solr/phenodigm/select?q=*:*&fq=type=disease&start=0&rows=1000000&wt=csv&fl=disease_term' \
  | tail -n+2 \
  | tr -d '"' \
  | sort -uf \
  | shuf -n 100 \
> sample_phenodigm.txt

# gene2phenotype
(for SOURCE in Cancer DD Eye Skin; do
    wget -qO- "https://www.ebi.ac.uk/gene2phenotype/downloads/${SOURCE}G2P.csv.gz"
done) \
  | gzip -d \
  | tail -n+2 \
  | csvtool format '%(3)\n' - \
  | sort -uf \
  | shuf -n 100 \
> sample_gene2phenotype.txt

# Sort and concatenate
cat sample_clinvar.txt sample_phenodigm.txt sample_gene2phenotype.txt | sort -uf > sample.txt
