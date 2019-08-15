/* small and rural*/

use "C:\Users\mlieng1\Documents\pat_data_and_analysis\local_transfers_dev\2019-07-16_2012_small_rural.dta"
mdesc


/*small & rural - - readiness vs. transferred*/
melogit transferred pr_score_cut_70, or
melogit transferred pr_score_cut_70 if !missing(sev_all_max_int, sex2), or
melogit transferred pr_score_cut_70 ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:
melogit transferred pr_score_cut_70 ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if(age_cats4_1==1), or || oshpd_id:

tab age_cats4
tab age_cats4==1
tab age_cats4!=1

/* note that one of the categories is missing from age_cats4_1 ("") */
melogit transferred pr_score_cut_70 if age_cats4==1 & !missing(sev_all_max_int, sex2), or
melogit transferred pr_score_cut_70  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4==1, or || oshpd_id:

melogit transferred pr_score_cut_70 if age_cats4!=1 & !missing(sev_all_max_int, sex2), or
melogit transferred pr_score_cut_70 ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4!=1, or || oshpd_id:

/*small & rural - - guidelines vs. transferred*/
melogit transferred i.pr_guidelines_agreements_factor if !missing(sev_all_max_int, sex2), or
melogit transferred i.pr_guidelines_agreements_factor  ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:

melogit transferred i.pr_guidelines2  ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:
melogit transferred i.pr_guidelines_agreements_factor  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4==1, or || oshpd_id:
melogit transferred i.pr_guidelines_agreements_factor ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4!=1, or || oshpd_id:

/* small & rural -- guidelines */
melogit transferred ib(last).pr_guidelines2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:
melogit transferred ib(last).pr_guidelines2  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4==1, or || oshpd_id:
melogit transferred ib(last).pr_guidelines2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4!=1, or || oshpd_id:


/* small & rural -- guidelines & agreements2 , reference = guidelines = TRUE*/
tabulate pr_guidelines_agreements2 transferred, chi2 row exact
melogit transferred i.pr_guidelines_agreements2, or
melogit transferred i.pr_guidelines_agreements2 if !missing(sev_all_max_int, sex2), or
melogit transferred i.pr_guidelines_agreements2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:
melogit transferred i.pr_guidelines_agreements2 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4==1, or || oshpd_id:
melogit transferred i.pr_guidelines_agreements2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4!=1, or || oshpd_id:

/* small & rural -- guidelines & agreements2 , reference = guidelines = FALSE*/
tabulate pr_guidelines_agreements2 transferred, chi2 row exact
melogit transferred ib2.pr_guidelines_agreements2, or
melogit transferred ib(last).pr_guidelines_agreements2 if !missing(sev_all_max_int, sex2), or
melogit transferred ib(last).pr_guidelines_agreements2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:
melogit transferred ib(last).pr_guidelines_agreements2 pr_score_cut_70 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:



melogit transferred ib(last).pr_guidelines_agreements2 if age_cats4==1 & !missing(sev_all_max_int, sex2), or
melogit transferred ib(last).pr_guidelines_agreements2 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4==1, or || oshpd_id:
melogit transferred ib(last).pr_guidelines_agreements2 if age_cats4!=1 & !missing(sev_all_max_int, sex2), or
melogit transferred ib(last).pr_guidelines_agreements2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4!=1, or || oshpd_id:



/* non-(small and rural)*/

 use "C:\Users\mlieng1\Documents\pat_data_and_analysis\local_transfers_dev\2019-07-16_2012_non_SR.dta"
mdesc


melogit transferred pr_score_cut_70, or
melogit transferred pr_score_cut_70 if !missing(sev_all_max_int, sex2, insurance3, hf_ALL_total_norm, hf_medicaid_norm), or
melogit transferred pr_score_cut_70 ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:
melogit transferred pr_score_cut_70 if !missing(sev_all_max_int, sex2, insurance3, hf_ALL_total_norm, hf_medicaid_norm) & age_cats4==1, or
melogit transferred pr_score_cut_70 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4==1, or || oshpd_id:
melogit transferred pr_score_cut_70 if !missing(sev_all_max_int, sex2, insurance3, hf_ALL_total_norm, hf_medicaid_norm) & age_cats4!=1, or
melogit transferred pr_score_cut_70 ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4!=1, or || oshpd_id:

melogit transferred i.pr_guidelines_agreements_factor, or
melogit transferred i.pr_guidelines_agreements_factor if !missing(sev_all_max_int, sex2, insurance3, hf_ALL_total_norm, hf_medicaid_norm), or
melogit transferred i.pr_guidelines_agreements_factor  ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:
melogit transferred i.pr_guidelines_agreements_factor  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4==1, or || oshpd_id:
melogit transferred i.pr_guidelines_agreements_factor  ib(last).age_cats4 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4!=1, or || oshpd_id:



/* small & rural -- guidelines & agreements2 , reference = guidelines = TRUE*/
tabulate pr_guidelines_agreements2 transferred, chi2 row exact
melogit transferred i.pr_guidelines_agreements2, or
melogit transferred i.pr_guidelines_agreements2 if !missing(sev_all_max_int, sex2), or
melogit transferred i.pr_guidelines_agreements2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:
melogit transferred i.pr_guidelines_agreements2 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4==1, or || oshpd_id:
melogit transferred i.pr_guidelines_agreements2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4!=1, or || oshpd_id:

/* small & rural -- guidelines & agreements2 , reference = guidelines = FALSE*/
tabulate pr_guidelines_agreements2 transferred, chi2 row exact
melogit transferred ib2.pr_guidelines_agreements2, or
melogit transferred ib(last).pr_guidelines_agreements2 if !missing(sev_all_max_int, sex2), or
melogit transferred ib(last).pr_guidelines_agreements2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm, or || oshpd_id:


melogit transferred ib(last).pr_guidelines_agreements2 if age_cats4==1 & !missing(sev_all_max_int, sex2), or
melogit transferred ib(last).pr_guidelines_agreements2 i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4==1, or || oshpd_id:
melogit transferred ib(last).pr_guidelines_agreements2 if age_cats4!=1 & !missing(sev_all_max_int, sex2), or
melogit transferred ib(last).pr_guidelines_agreements2 ib(last).age_cats4  i.sex2 i.race_group3 ib2.insurance3 sev_all_max_int has_ccc_any hf_ALL_total_norm hf_medicaid_norm if age_cats4!=1, or || oshpd_id:
