library(dplyr)

# Calculate 'sum of metrics'
ancient_authors_ranking <- ancient_authors_wikidata_item_metrics %>%
  dplyr::left_join(
    ancient_authors_wikidata_with_precision %>%
      dplyr::select(wikidata_id, name = name_en),
    by = "wikidata_id"
  ) %>%
  dplyr::mutate(
    `sum of metrics` = rowSums(dplyr::across(c(statements, identifiers, total_sitelinks)), na.rm = TRUE),
    rank = dplyr::min_rank(dplyr::desc(`sum of metrics`))
  ) %>%
  dplyr::select(rank, name, wikidata_id, statements, identifiers, total_sitelinks, `sum of metrics`)

# Create dataframe for table
top30_final <- ancient_authors_ranking %>%
  dplyr::arrange(rank) %>%
  dplyr::slice(1:30)

# Create LaTeX table
latex_final_ranking <- knitr::kable(top30_final, format = "latex", booktabs = TRUE)

# Print or copy to clipboard
cat(latex_final)