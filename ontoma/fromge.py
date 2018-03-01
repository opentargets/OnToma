#    def execute_ge_request(self, sample=TEST_SAMPLE):
#         '''
#         Create panel app info list and phenotype set
#         :return: Unique phenotype list
#         '''
#         self._logger.warning("execute_ge_request...")
#         phenotype_list = []
#         nb_panels = 0
#         for panel_name, panel_id, panel_version, panel_diseasegroup, panel_diseasesubgroup in self.request_to_panel_app():
#             nb_panels +=1
#             self._logger.warning("reading panel %s %s %s %s %s" % (panel_name, panel_id, panel_version, panel_diseasegroup, panel_diseasesubgroup))
#             url = 'https://panelapp.genomicsengland.co.uk/WebServices/search_genes/all/'
#             r = requests.get(url, params={"panel_name": panel_name}, timeout=30)
#             results = r.json()
#             for item in results['results']:

#                 ensembl_iri = None

#                 ensembl_gene_id = self.genes.get(item['GeneSymbol'])
#                 if not ensembl_gene_id:
#                     self._logger.error("%s is not found in Ensembl" % item['GeneSymbol'])
#                     continue

#                 ensembl_iri = "http://identifiers.org/ensembl/" + ensembl_gene_id
#                 self._logger.warning("Gene %s %s" % (item['GeneSymbol'], ensembl_iri))

#                 if ensembl_iri and item['EnsembleGeneIds'] and item['Phenotypes'] and item['LevelOfConfidence'] == 'HighEvidence':
#                     for element in item['Phenotypes']:

#                         element = element.rstrip().lstrip().rstrip("?")
#                         if len(element) > 0:

#                             '''
#                             First check whether it's an OMIM identifier
#                             '''
#                             match_omim = re.match('^(\d{6,})$', element)
#                             match_omim2 = re.match('^\s*OMIM:?[#\s]*(\d{6,})$', element)
#                             if match_omim or match_omim2:
#                                 if match_omim:
#                                     omim_id = match_omim.groups()[0]
#                                 elif match_omim2:
#                                     omim_id = match_omim2.groups()[0]
#                                 self._logger.info("Found OMIM ID: %s" % (omim_id))
#                                 if omim_id in self.omim_to_efo_map:
#                                     self._logger.info("Maps to EFO")
#                                     for mapping in self.omim_to_efo_map[omim_id]:
#                                         disease_label = mapping['efo_label']
#                                         disease_uri = mapping['efo_uri']
#                                         self.panel_app_info.append([panel_name,
#                                                                     panel_id,
#                                                                     panel_version,
#                                                                     panel_diseasegroup,
#                                                                     panel_diseasesubgroup,
#                                                                     item['GeneSymbol'],
#                                                                     item['EnsembleGeneIds'][0],
#                                                                     ensembl_iri,
#                                                                     item['LevelOfConfidence'],
#                                                                     element,
#                                                                     omim_id,
#                                                                     item['Publications'],
#                                                                     item['Evidences'],
#                                                                     [omim_id],
#                                                                     disease_uri,
#                                                                     disease_label,
#                                                                     "OMIM MAPPING"
#                                                                     ])
#                                 self.map_strings = "%s\t%s\t%s\t%s\t%s\t%s"%(panel_name, item['GeneSymbol'], item['LevelOfConfidence'], element, disease_uri, disease_label)
#                             else:

#                                 '''
#                                 if there is already an OMIM xref to EFO/Orphanet, no need to map
#                                 '''
#                                 disease_uri = None
#                                 disease_label = None
#                                 is_hpo = False
#                                 is_efo = False

#                                 omim_ids = []
#                                 phenotype_label = None
#                                 mapping_type = "EFO MAPPING"

#                                 match_hpo_id = re.match('^(.+)\s+(HP:\d+)$', element)
#                                 match_curly_brackets_omim = re.match('^{([^\}]+)},\s+(\d+)', element)
#                                 match_no_curly_brackets_omim = re.match('^(.+),\s+(\d{6,})$', element)
#                                 match_no_curly_brackets_omim_continued = re.match('^(.+),\s+(\d{6,})\s+.*$', element)
#                                 # Myopathy, early-onset, with fatal cardiomyopathy 611705
#                                 match_no_curly_brackets_no_comma_omim = re.match('^(.+)\s+(\d{6,})\s*$', element)
#                                 if element.lower() in self.efo_labels:
#                                     disease_uri = self.efo_labels[element.lower()]
#                                     disease_label = element
#                                     phenotype_label = disease_label
#                                     is_efo = True
#                                     mapping_type = "EFO MAPPING"
#                                 elif element.lower() in self.hpo_labels:
#                                     disease_uri = self.hpo_labels[element.lower()]
#                                     disease_label = self.hpo.current_classes[disease_uri]
#                                     phenotype_label = disease_label
#                                     is_hpo = True
#                                     mapping_type = "HPO MAPPING (in HPO labels)"
#                                 elif match_hpo_id:
#                                     # Ichthyosis HP:64
#                                     disease_label = match_hpo_id.groups()[0]
#                                     phenotype_label = disease_label
#                                     phenotype_id = match_hpo_id.groups()[1]
#                                     disease_uri = "http://purl.obolibrary.org/obo/" + phenotype_id.replace(":", "_")
#                                     if disease_uri in self.hpo.current_classes:
#                                         disease_label = self.hpo.current_classes[disease_uri]
#                                         is_hpo = True
#                                     mapping_type = "HPO MAPPING (match HPO id)"
#                                 elif self.search_request_to_ols(query=element) is True:
#                                     disease_uri = self.ols_synonyms[element]["iri"]
#                                     disease_label = self.ols_synonyms[element]["label"]
#                                     phenotype_label = disease_label
#                                     is_hpo = True
#                                     mapping_type = "HPO MAPPING (request to OLS)"
#                                 elif match_curly_brackets_omim:
#                                     #[{Pancreatitis, idiopathic}, 167800]
#                                     phenotype_label = match_curly_brackets_omim.groups()[0]
#                                     omim_ids.append(match_curly_brackets_omim.groups()[1])
#                                     mapping_type = "OMIM MAPPING"
#                                 elif match_no_curly_brackets_omim:
#                                     #[{Pancreatitis, idiopathic}, 167800]
#                                     phenotype_label = match_no_curly_brackets_omim.groups()[0]
#                                     omim_ids.append(match_no_curly_brackets_omim.groups()[1])
#                                     mapping_type = "OMIM MAPPING"
#                                 elif match_no_curly_brackets_omim_continued:
#                                     #[{Pancreatitis, idiopathic}, 167800]
#                                     phenotype_label = match_no_curly_brackets_omim_continued.groups()[0]
#                                     omim_ids.append(match_no_curly_brackets_omim_continued.groups()[1])
#                                     mapping_type = "OMIM MAPPING"
#                                 elif match_no_curly_brackets_no_comma_omim:
#                                     #[{Pancreatitis, idiopathic}, 167800]
#                                     phenotype_label = match_no_curly_brackets_no_comma_omim.groups()[0]
#                                     omim_ids.append(match_no_curly_brackets_no_comma_omim.groups()[1])
#                                     mapping_type = "OMIM MAPPING"
#                                 else:
#                                     phenotype_label = None
#                                     #if isinstance(element, str):
#                                     phenotype_label = element.strip()
#                                     #else:
#                                     #    phenotype_label = element.decode('iso-8859-1').encode('utf-8').strip()
#                                     phenotype_label = re.sub(r"\#", "", phenotype_label)
#                                     phenotype_label = re.sub(r"\t", "", phenotype_label)
#                                     omim_ids = re.findall(r"\d{5}",phenotype_label)
#                                     phenotype_label = re.sub(r"\d{5}", "", phenotype_label)
#                                     phenotype_label = re.sub(r"\{", "", phenotype_label)
#                                     phenotype_label = re.sub(r"\}", "", phenotype_label)
#                                     mapping_type = "GENERAL MAPPING"

#                                 self._logger.info("[%s] => [%s]" % (element, phenotype_label))



#                                 if omim_ids is None:
#                                     omim_ids = []

#                                 self.map_omim[phenotype_label] = omim_ids



#                                 if not is_hpo and not is_efo and all(l not in self.omim_to_efo_map for l in omim_ids) and phenotype_label.lower() not in self.zooma_to_efo_map:
#                                     self._logger.info("Unknown term '%s' with unknown OMIM ID(s): %s"%(phenotype_label, ";".join(omim_ids)))
#                                     phenotype_list.append(phenotype_label)

#                                     self.panel_app_info.append([panel_name,
#                                                                 panel_id,
#                                                                 panel_version, panel_diseasegroup, panel_diseasesubgroup,
#                                                                 item['GeneSymbol'],
#                                                                 item['EnsembleGeneIds'][0],
#                                                                 ensembl_iri,
#                                                                 item['LevelOfConfidence'],
#                                                                 element,
#                                                                 phenotype_label,
#                                                                 item['Publications'],
#                                                                 item['Evidences'],
#                                                                 omim_ids,
#                                                                 disease_uri,
#                                                                 disease_label,
#                                                                 mapping_type])

#                                 else:
#                                     self._logger.info("THERE IS A MATCH")

#                                     if is_hpo or is_efo:
#                                         self.panel_app_info.append([panel_name,
#                                                                     panel_id,
#                                                                     panel_version, panel_diseasegroup,
#                                                                     panel_diseasesubgroup,
#                                                                     item['GeneSymbol'],
#                                                                     item['EnsembleGeneIds'][0],
#                                                                     ensembl_iri,
#                                                                     item['LevelOfConfidence'],
#                                                                     element,
#                                                                     phenotype_label,
#                                                                     item['Publications'],
#                                                                     item['Evidences'],
#                                                                     omim_ids,
#                                                                     disease_uri,
#                                                                     disease_label,
#                                                                     mapping_type])

#                                     elif omim_ids and any(l  in self.omim_to_efo_map for l in omim_ids):
#                                         for omim_id in omim_ids:
#                                             if omim_id in self.omim_to_efo_map:
#                                                 for mapping in self.omim_to_efo_map[omim_id]:
#                                                     disease_label = mapping['efo_label']
#                                                     disease_uri = mapping['efo_uri']

#                                                     self.panel_app_info.append([panel_name,
#                                                                                 panel_id,
#                                                                                 panel_version, panel_diseasegroup,
#                                                                                 panel_diseasesubgroup,
#                                                                                 item['GeneSymbol'],
#                                                                                 item['EnsembleGeneIds'][0],
#                                                                                 ensembl_iri,
#                                                                                 item['LevelOfConfidence'],
#                                                                                 element,
#                                                                                 phenotype_label,
#                                                                                 item['Publications'],
#                                                                                 item['Evidences'],
#                                                                                 omim_ids,
#                                                                                 disease_uri,
#                                                                                 disease_label,
#                                                                                 mapping_type])

#                                     elif phenotype_label.lower() in self.zooma_to_efo_map:
#                                         for mapping in self.zooma_to_efo_map[phenotype_label.lower()]:

#                                             disease_label = mapping['efo_label']
#                                             disease_uri = mapping['efo_uri']

#                                             self.panel_app_info.append([panel_name,
#                                                                         panel_id,
#                                                                         panel_version, panel_diseasegroup,
#                                                                         panel_diseasesubgroup,
#                                                                         item['GeneSymbol'],
#                                                                         item['EnsembleGeneIds'][0],
#                                                                         ensembl_iri,
#                                                                         item['LevelOfConfidence'],
#                                                                         element,
#                                                                         phenotype_label,
#                                                                         item['Publications'],
#                                                                         item['Evidences'],
#                                                                         omim_ids,
#                                                                         disease_uri,
#                                                                         disease_label,
#                                                                         mapping_type])




#                                 self.map_strings = "%s\t%s\t%s\t%s\t%s\t%s" % (
#                                 panel_name, item['GeneSymbol'], item['LevelOfConfidence'], element, disease_uri,
#                                 disease_label)

#             self.phenotype_set = set(phenotype_list)

#             if sample == True:
#                 print(json.dumps(self.panel_app_info, indent=2))
#                 break

#         return self.phenotype_set