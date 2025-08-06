## ------------------------------------------------------------------------------------------------------------------
## Script name: create_text2icasa_training_set.R
## Purpose of script: generate jsonl training file for LLM-supported metadata extraction from tokenized pdf files to
## the ICASA data format
##
## Author: Benjamin Leroy
## Date Created: 2025-08-06
## Email: benjamin.leroy@tum.de
## ------------------------------------------------------------------------------------------------------------------
## Notes:
## 2025-08-04:
## - The script can be edited for to extract short instead of long ICASA column names. Short names consume less token
## but they likely complicate the task as they are less explicit
## - The script can be edited to manually exclude ICASA variables by adding them to the 'todrop' vector
## - The training data generating function offers two options:
##   - All template columns were included (i.e., ticked in Excel workbook) for comprehensiveness
##   - One-to-one (each experimental year is paired with the entire pdf, with the extraction task focused on data
##     for the focal year); generally recommended (lower single-pair token count, possibly more accurate)
##   - One-to-many: all experimental years are bundled together and paired with the pdf
## 
## ------------------------------------------------------------------------------------------------------------------

# ---- Install/load libraries

#install.packages("devtools")  # Run once; helper to install functions from GitHub
library(devtools)
install_github("leroy-bml/csmTools")  # Run only to install or update the package (install all dependencies when prompted)
library(csmTools)

library(openxlsx2)
library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(jsonlite)


# ---- Extract from template
template_path <- "./data/0_traning_set/0_template/template_icasa_vba_trainingSet_allColumns.xlsm"  # <-- template file path
header = "long"  # <-- set whether to use long or short ICASA headers

str_datasets <- extract_template(path = template_path,
                                 headers = "long", 
                                 keep_empty = TRUE,
                                 keep_null_events = TRUE)

# Load and format name dictionary
template <- suppressWarnings(wb_load(template_path))

dict <- wb_to_df(template, sheet = "Dictionary", startRow = 1)
dict$name = if (header == "long") dict$var_name else dict$Code_Query
dict <- select(dict, c(Sheet, name))


# ---- Reorder columns
reorder_columns <- function(ls, name_dict) {
  lapply(names(ls), function(sec) {
    df <- ls[[sec]]
    desired_order <- name_dict$name[name_dict$Sheet == sec]
    ordered_cols <- intersect(desired_order, names(df))
    df[, ordered_cols, drop = FALSE]
  }) |> setNames(names(ls))
}
data_reordered <- lapply(str_datasets, function(ls) reorder_columns(ls, name_dict = dict))
names(data_reordered) <- names(str_datasets)


# ---- Merge subsections with headers: using the ICASA 'Group' level for model training
merge_sections <- function(ls) {
  
  # Remove measured data for now...
  ls <- ls[!grepl("SM_|TS_|OBS", names(ls))]
  
  merge_map <- list(
    INITIAL_CONDITIONS = list("INITIAL_CONDITION_LAYER", c("experiment_ID", "initial_conditions_level")),
    IRRIGATIONS = list("IRRIGATION_APPLICATIONS", c("experiment_ID", "irrigation_level")),
    FERTILIZERS = list("FERTILIZER_APPLICS", c("experiment_ID", "fertilizer_level")),
    ORGANIC_MATERIALS = list("ORGANIC_MATERIAL_APPLICS", c("experiment_ID", "org_materials_applic_lev")),
    TILLAGE = list("TILLAGE_EVENTS", c("experiment_ID", "tillage_level")),
    SOIL_PROFILES = list("SOIL_PROFILE_LAYERS", "soil_profile_ID"),
    SOIL_ANALYSES = list("SOIL_ANALYSES_LAYERS", c("experiment_ID", "soil_analysis_level")),
    CHEMICALS = list("CHEMICAL_APPLICS", c("experiment_ID", "chemical_applic_level")),
    ENVIRON_MODIFICATIONS = list("ENVIRON_MODIF_LEVELS", c("experiment_ID", "environmental_modif_lev")),
    HARVESTS = list("HARVEST_EVENTS", c("experiment_ID", "harvest_operations_level"))
  )
  
  merged <- list()
  for (main in names(merge_map)) {
    #print(main)  # debug
    sub <- merge_map[[main]][[1]]
    keys <- merge_map[[main]][[2]]
    
    main_df <- ls[[main]]
    sub_df  <- ls[[sub]]
    
    if (!is.null(main_df) && !is.null(sub_df)) {
      if (nrow(main_df) > 0 && nrow(sub_df) > 0) {
        # Perform left join
        merged[[main]] <- dplyr::left_join(main_df, sub_df, by = keys)
      } else {
        # Perform bind_cols and remove duplicate columns
        # Note: suppress new column notices to allow debug print
        combined <- suppressMessages(dplyr::bind_cols(main_df, sub_df))
        clean_names <- gsub("\\.\\.\\.\\d+$", "", names(combined))
        deduped <- combined[, !duplicated(clean_names)]
        names(deduped) <- gsub("\\.\\.\\.\\d+$", "", names(deduped))
        merged[[main]] <- deduped
        
      }
    } else if (!is.null(main_df)) {
      merged[[main]] <- main_df
    }
  }
  
  head <- names(lapply(merge_map, function(x) x[[1]]))
  deps <- unname(do.call(c, lapply(merge_map, function(x) x[[1]])))
  out_nm <- setdiff(names(ls), c(head, deps))
  
  out <- c(ls[out_nm], merged)
  
  return(out)
}

data_merged <- list()
for (nm in names(data_reordered)) {
  print(nm)  # debug
  ls <- data_reordered[[nm]]
  data_merged[[nm]] <- merge_sections(ls)
}

# ---- Make treatment name matrix
process_treatment_matrix <- function(ls) {
  
  treatments <- ls[["TREATMENTS"]]
  treatment_lev_nms <- names(treatments)[grepl("lev", names(treatments))]  # join keys
  
  mngt_dfs <- keep(ls, ~ any(treatment_lev_nms %in% names(.x)))
  mngt_dfs <- mngt_dfs[!names(mngt_dfs) %in% "TREATMENTS"]
  
  # Make level-name maps
  name_map_dfs <- map(mngt_dfs, function(df) {
    
    level_col <- grep("lev", names(df), value = TRUE)
    level_col <- grep("name|elevation", level_col, value = TRUE, invert = TRUE)  # remove inconsistencies
    
    name_col <- grep("name", names(df), value = TRUE)
    name_col <- grep("operation_name|app_name|season_name", name_col, value = TRUE, invert = TRUE)  # remove inconsistencies
    
    df_out <- select(df, all_of(c(level_col, name_col)))
    df_out <- distinct(df_out)
    
    # Set ID to 0 for join when no data
    if (nrow(df_out) == 0) {
      df_out <- tibble(!!level_col := 0, !!name_col := NA_character_)
    }
    
    return(df_out)
  })
  
  # Sequential join to treatment matrix
  treatments <- suppressMessages(
    reduce(
      .x = name_map_dfs,
      .f = left_join,
      .init = treatments
    )
  )
  # Drop levels to get a treatment matrix by name
  treatments <- select(treatments, -all_of(treatment_lev_nms))
  
  ls[["TREATMENTS"]] <- treatments
  return(ls)
}

data_merged_names <- list()
for (nm in names(data_merged)) {
  print(nm)  # debug
  ls <- data_merged[[nm]]
  data_merged_names[[nm]] <- process_treatment_matrix(ls)
}

# ---- Drop IDs (experiment_ID only kept in METADATA table)
todrop <- c("experiment_ID", "institute_ID", "people_level", "document_ID", "revision_notes", 
            "field_level", "weather_sta_identifier", "soil_identifier", "field_id",
            "cultivar_level", "cultivar_identifier",
            "planting_level",
            "treatment_number",
            "soil_profile_ID", "document_ID_sl", "revision_notes_sl",
            "document_ID_wst",
            "initial_conditions_level",
            "irrigation_level",
            "fertilizer_level",
            "tillage_level",
            "soil_analysis_level",
            "chemical_applic_level",
            "environmental_modif_lev",
            "harvest_operations_level")  # NB: add here any column you wish to exclude from training set

data_merged_names_noid <- lapply(data_merged_names, function(ls) {
  lapply(names(ls), function(nm) {
    df <- ls[[nm]]
    drop_vars <- if (nm == "EXP_METADATA") setdiff(todrop, "experiment_ID") else todrop
    df[, !(names(df) %in% drop_vars), drop = FALSE]
  }) |> setNames(names(ls))
})


# ---- Generate json outputs

# Helper to keep empty dataframes in
fix_empty_df <- function(df) {
  if (is.data.frame(df) && nrow(df) == 0) {
    # Create a one-row data frame with NA for each column
    as.data.frame(lapply(df, function(x) NA), stringsAsFactors = FALSE)
  } else {
    df
  }
}
generate_json <- function(group_list) {
  # Apply fix to each subsection
  fixed_group <- lapply(group_list, fix_empty_df)
  toJSON(fixed_group, pretty = TRUE, na = "null", Date = "ISO8601", POSIXt = "ISO8601")
}
str_data_json <- lapply(data_merged_names_noid, generate_json)

# Write files
lapply(names(str_data_json), function(name) {
  writeLines(str_data_json[[name]], con = file.path("./data/0_traning_set/1_template_json", paste0(name, ".json")))
})

# ---- Create training pairs

# Helper function to generate training file
generate_training_file <- function(md_folder, str_folder, output_file, method = "one_to_many") {
  
  md_files <- list.files(md_folder, pattern = "\\.md$", full.names = TRUE)
  str_files <- list.files(str_folder, pattern = "\\.json$", full.names = TRUE)
  
  # Ensure the output file is clean before starting
  if (file.exists(output_file)) {
    file.remove(output_file)
  }
  
  for (md_path in md_files) {
    
    # Derive base name (e.g., "AuthorYYYY" from ".../AuthorYYYY.md")
    base_name <- tools::file_path_sans_ext(basename(md_path))
    
    # Find all corresponding JSON files for this paper
    matching_json_basenames <- grep(paste0("^", base_name, "_"), basename(str_files), value = TRUE)
    
    if (length(matching_json_basenames) == 0) {
      warning(paste("No structured JSON found for:", basename(md_path)))
      next
    }
    
    # Reconstruct the full paths for the matching JSON files
    json_paths <- file.path(str_folder, matching_json_basenames)
    
    # Read the unstructured text content once per paper
    unstructured_text <- paste(readLines(md_path, warn = FALSE), collapse = "\n")
    
    # Method One-to-Many (One pair per year)
    if (method == "one_to_many") {
      for (json_path in json_paths) {

        structured_data_string <- paste(readLines(json_path, warn = FALSE), collapse = "\n")
        # Create the prompt-completion pair
        final_pair <- list(prompt = unstructured_text, completion = structured_data_string)
        # Convert to a single JSON line and write to the output file
        json_line <- toJSON(final_pair, auto_unbox = TRUE)
        write(json_line, file = output_file, append = TRUE)
      }
    } 
    # Method One-to-One (One pair per paper)
    else if (method == "one_to_one") {
      # This list will hold the parsed R objects for each year's JSON
      combined_r_objects <- list()
      
      for (json_path in json_paths) {
        year_match <- str_match(basename(json_path), "_(\\d{4})\\.json$")
        if (is.na(year_match[1, 2])) {
          warning(paste("Could not extract year from filename:", basename(json_path)))
          next
        }
        year_key <- paste0("year_", year_match[1, 2])
        
        structured_data_string <- paste(readLines(json_path, warn = FALSE), collapse = "\n")
        structured_object <- fromJSON(structured_data_string, simplifyVector = FALSE)
        combined_r_objects[[year_key]] <- structured_object
      }
      
      if (length(combined_r_objects) > 0) {
        completion_string <- toJSON(combined_r_objects, auto_unbox = TRUE, pretty = TRUE)
        # Create the final prompt-completion pair
        final_pair <- list(prompt = unstructured_text, completion = completion_string)
        # Convert to a single JSON line and write to the output file
        json_line <- toJSON(final_pair, auto_unbox = TRUE)
        write(json_line, file = output_file, append = TRUE)
      }
    }
  }
  
  invisible(output_file)
}

# SCENARIO #1: one-to-one = one large training pair per pdf
# >>> less total token count (should be no token limit for total size anyways)
# >>> higher risk of single-response token limit
# >>> less focused learning task (less accurate??)
generate_training_file(
  md_folder = "./data/0_traning_set/0_tokenized_pdfs",  # markown folder
  str_folder = "./data/0_traning_set/1_template_json",  # json folder
  output_file = "./data/0_traning_set/text2icasa_traning_data_1to1.jsonl",  # training data name
  method = "one_to_one" 
)

# SCENARIO #2: one small training pair per experimental year (i.e., n_year pairs per pdf)
# >>> higher total token count (should be no token limit for total size anyways)
# >>> lower risk of single-response token limit (single training files are smaller)
# >>> more focused learning task (potantially more accurate)
generate_training_file(
  md_folder = "./data/0_traning_set/0_tokenized_pdfs",  # markown folder
  str_folder = "./data/0_traning_set/1_template_json",  # json folder
  output_file = "./data/0_traning_set/text2icasa_traning_data_1toMany.jsonl",  # training data name
  method = "one_to_many" 
)


# --- 2025-08-06: Corrected errors in template (both normal and allColumn versions)
# Dettori2017_1975 duplicate series of harvest events (1-6) -> changed to 1976 (was missing)
# Fernando rainfed no irrigation records need to be matched in the applics sheet
# Ventrella init cond same
# Single files experiment need experimental year in name to comply with name convention
## -> added for Spiertz1978, Darwinkel1978, Boulch2021 (NB: year unknown so 9999)

# --- To correct
# Keep all variables in environment modifications
# SIMULATION_CONTROLS/SIMULATION_PARAMETERS tables?
