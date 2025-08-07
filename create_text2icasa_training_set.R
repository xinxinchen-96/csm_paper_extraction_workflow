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
## - All template columns were included (i.e., ticked in Excel workbook) for comprehensiveness
## - The training data generating function offers two options:
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
template_path <- "./data/0_training_set/0_template/template_icasa_vba_trainingSet_allColumns.xlsm"  # <-- template file path
header = "long"  # <-- set whether to use long or short ICASA headers

str_datasets <- extract_template(path = template_path,
                                 headers = header,  # TODO: short does not work, gotta fix it...
                                 keep_empty = TRUE,
                                 keep_null_events = TRUE)

# Load and format name dictionary
template <- suppressWarnings(wb_load(template_path))

dict <- wb_to_df(template, sheet = "Dictionary", startRow = 1)
dict$name = if (header == "long") dict$var_name else dict$Code_Query
dict <- select(dict, c(Sheet, name))


# ---- Reorder columns following dictionary order
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


# ---- Merge subsections with headers
# NB: the ICASA 'Group' level is used for grouping variables for model training

included_sections <- unique(unlist(lapply(data_reordered, names)))  # all sections
#included_sections <- c("EXP_METADATA","FIELDS")  # <-- Fill out this argument to select what to keep from the template

# NB: always keep/drop header sections (e.g., FERTILIZERS) together with their respective
# subsection, otherwise it won't work further!

merge_sections <- function(ls, headers = "short", sections = NULL) {
  
  # Remove measured data for now...
  ls <- ls[!grepl("SM_|TS_|OBS", names(ls))]  #tmp hack, to ensure measured data dropped in any case
  ls <- ls[included_sections]

  if (headers == "long") {
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
  } else {
    merge_map <- list(
      INITIAL_CONDITIONS = list("INITIAL_CONDITION_LAYER", c("EXPER_ID", "IC")),
      IRRIGATIONS = list("IRRIGATION_APPLICATIONS", c("EXPER_ID", "IR")),
      FERTILIZERS = list("FERTILIZER_APPLICS", c("EXPER_ID", "FE")),
      ORGANIC_MATERIALS = list("ORGANIC_MATERIAL_APPLICS", c("EXPER_ID", "OM")),
      TILLAGE = list("TILLAGE_EVENTS", c("EXPER_ID", "TI")),
      SOIL_PROFILES = list("SOIL_PROFILE_LAYERS", "SOIL_ID"),
      SOIL_ANALYSES = list("SOIL_ANALYSES_LAYERS", c("EXPER_ID", "SA")),
      CHEMICALS = list("CHEMICAL_APPLICS", c("EXPER_ID", "CH")),
      ENVIRON_MODIFICATIONS = list("ENVIRON_MODIF_LEVELS", c("EXPER_ID", "EM")),
      HARVESTS = list("HARVEST_EVENTS", c("EXPER_ID", "HA"))
    )
  }
  
  

  
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
  data_merged[[nm]] <- merge_sections(ls, headers = header, sections = included_sections)
}

# ---- Make treatment name matrix ------------------------------------------------------------------------------------

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

# ---- Drop IDs (experiment_ID only kept in METADATA table) ----------------------------------------------------------

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
            "harvest_operations_level")  # <--- NB: add here any column you wish to exclude from training set

data_merged_names_noid <- lapply(data_merged_names, function(ls) {
  lapply(names(ls), function(nm) {
    df <- ls[[nm]]
    drop_vars <- if (nm == "EXP_METADATA") setdiff(todrop, "experiment_ID") else todrop
    df[, !(names(df) %in% drop_vars), drop = FALSE]
  }) |> setNames(names(ls))
})


# ---- Generate json outputs -----------------------------------------------------------------------------------------

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
  writeLines(str_data_json[[name]], con = file.path("./data/0_training_set/1_template_json", paste0(name, ".json")))
})

# ---- Create training files -----------------------------------------------------------------------------------------

# Fetch and format ICASA dictionary
fetch_icasa <- function(url) {
  
  # List of CSV filenames (you can expand this list or scrape it dynamically)
  csv_files <- c("Metadata.csv", "Management_info.csv", "Soils_data.csv", "Weather_data.csv", "Measured_data.csv")
  
  # Read all CSVs into a named list
  data_list <- lapply(csv_files, function(file) {
    url <- paste0(url, file)
    read.csv(url)
  })
  
  out <- do.call(rbind, data_list)
  out <- cbind(out["var_uid"], out[ , setdiff(names(out), "var_uid")])
  row.names(out) <- NULL
  
  return(out)
}
icasa_dict <- fetch_icasa("https://raw.githubusercontent.com/DSSAT/ICASA-Dictionary/main/CSV/")

icasa_dict <- icasa_dict %>%
  filter(Subset %in% c(  # <-- Add/remove ICASA sections you want to include/exclude in the training job
    "METADATA",
    "MANAGEMENT",
    "SOIL_METADATA", "SOIL_PROFILES",
    "WEATHER_METADATA", "WEATHER_STATION",
    "MEASURED_DATA"
    )) %>%
  #filter(Group %in% c("FIELDS","FERTILIZERS")) %>%  # <-- If you want to generate training file for different agents
  mutate(section = Group,
         variable_name = Variable_Name,  # <-- Change "Variable_Name" to "Code_Display" for short names
         description = Description,
         unit_or_type = Unit_or_type) %>%
  filter(!variable_name %in% todrop) %>%  # <-- Remove IDs and variable excluded from mapping earlier
  # TODO: add names to the TREATMENTS matrix!
  mutate(json_type = case_when(
    unit_or_type %in% c("text","code","date[yyyy]","date","%+code") ~ "string",
    unit_or_type %in% c("number","year","doy","dap") ~ "integer",
    # TODO: find booleans!
    TRUE ~ "number"
  )) %>%
  select(section, variable_name, description, json_type, unit_or_type)

# Generate GPT4o standard ICASA schema
generate_icasa_schema <- function(dict) {

  required_cols <- c("section", "variable_name", "description", "json_type", "unit_or_type")
  if (!all(required_cols %in% names(dict))) {
    stop("CSV must contain columns: 'section', 'variable_name', 'description', 'json_type', and 'unit_or_type'")
  }
  
  properties_for_single_exp <- list()
  sections <- unique(dict$section)
  
  for (current_section in sections) {
    section_vars_df <- subset(dict, section == current_section)
    item_properties <- list()
    for (i in 1:nrow(section_vars_df)) {
      row <- section_vars_df[i, ]
      prop_list <- list(type = row$json_type, description = row$description)
      if (!is.na(row$unit_or_type)) {
        prop_list$unit_or_type <- row$unit_or_type
      }
      item_properties[[row$variable_name]] <- prop_list
    }
    
    section_schema <- list(
      type = "array",
      description = paste("Data related to", current_section),
      items = list(type = "object", properties = item_properties)
    )
    properties_for_single_exp[[current_section]] <- section_schema
  }
  
  single_experiment_schema <- list(
    type = "object",
    properties = properties_for_single_exp
  )

  final_schema <- list(
    type = "object",
    properties = list(
      experiments = list(
        type = "array",
        description = "An array containing all unique crop-year combinations found in the article.",
        items = single_experiment_schema
      )
    ),
    required = c("cropYear")
  )
  
  return(final_schema)
}
icasa_schema <- generate_icasa_schema(icasa_dict)

# Assemble tool definition payload
tool_definition <- list(
  type = "function",
  `function` = list(
    name = "text_to_icasa",
    description = paste(  # <-- Specify your training instructions
      "Extracts and structures data from a scientific article about a crop field or modeling experiment according to the ICASA data model.",
      "The ICASA data model (provided in parameters) provide a list of standard variable names in several sections, with their respective unit and data type.",
      "The source text may describe experiments spanning multiple crops or multiple experimental years.",
      "You MUST create a separate and complete cropYear object for each unique combination of a crop and a growing season (i.e., experimental year, defined as the year of harvest) found in the text.",
      "All extracted terms must be valid terms from the ICASA controlled vocabulary. For numeric properties (e.g., yields) units must be extracted and convert to the target unit, provided in the unit_or_type property.",
      "If a specific variable is not mentioned in the text for a given experiment, return null for this variable."
    ),
    parameters = icasa_schema  # <-- the ICASA dictionary we generated in step ##
  )
)

# Generate training dataset
generate_training_file <- function(md_folder, str_folder, output_file, tool_definition, method = "one_to_many") {
  
  md_files <- list.files(md_folder, pattern = "\\.md$", full.names = TRUE, ignore.case = TRUE)
  str_files <- list.files(str_folder, pattern = "\\.json$", full.names = TRUE, ignore.case = TRUE)
  
  if (file.exists(output_file)) {
    file.remove(output_file)
  }
  
  tool_name <- tool_definition$`function`$name
  
  for (md_path in md_files) {
    base_name <- tools::file_path_sans_ext(basename(md_path))
    matching_json_basenames <- grep(paste0("^", base_name, "_"), basename(str_files), value = TRUE)
    
    if (length(matching_json_basenames) == 0) {
      warning(paste("No structured JSON found for:", basename(md_path)))
      next
    }
    
    json_paths <- file.path(str_folder, matching_json_basenames)
    unstructured_text <- paste(readLines(md_path, warn = FALSE), collapse = "\n")
    
    create_jsonl_entry <- function(user_content, tool_arguments_string) {
      final_structure <- list(
        messages = list(
          list(role = "user", content = user_content),
          list(
            role = "assistant",
            tool_calls = list(
              list(
                id = paste0("call_", gsub("[^a-zA-Z0-9]", "", base_name), "_", sample(1e6, 1)),
                type = "function",
                `function` = list(
                  name = tool_name,
                  arguments = tool_arguments_string
                )
              )
            )
          )
        ),
        tools = list(tool_definition),
        parallel_tool_calls = FALSE
      )
      return(jsonlite::toJSON(final_structure, auto_unbox = TRUE))
    }
    
    if (method == "one_to_many") {
      for (json_path in json_paths) {
        structured_data_string <- paste(readLines(json_path, warn = FALSE), collapse = "\n")
        json_line <- create_jsonl_entry(unstructured_text, structured_data_string)
        write(json_line, file = output_file, append = TRUE)
      }
    } else if (method == "one_to_one") {
      crop_year_ls <- list()
      for (json_path in json_paths) {
        # Each JSON file is one experiment/year
        structured_object <- jsonlite::fromJSON(json_path, simplifyVector = FALSE)
        crop_year_ls <- append(crop_year_ls, list(structured_object))
      }
      
      if (length(crop_year_ls) > 0) {
        final_combined_object <- list(cropYear = crop_year_ls)
        
        combined_json_string <- jsonlite::toJSON(final_combined_object, auto_unbox = TRUE, pretty = TRUE)
        json_line <- create_jsonl_entry(unstructured_text, combined_json_string)
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
  md_folder = "./data/0_training_set/0_tokenized_pdfs",  # markown folder
  str_folder = "./data/0_training_set/1_template_json",  # json folder
  output_file = "./data/0_training_set/text2icasa_training_data_1to1.jsonl",  # training data name
  tool_definition = tool_definition,  # the ICASA dict
  method = "one_to_one" 
)

# SCENARIO #2: one small training pair per experimental year (i.e., n_year pairs per pdf)
# >>> higher total token count (should be no token limit for total size anyways)
# >>> lower risk of single-response token limit (single training files are smaller)
# >>> more focused learning task (potantially more accurate)
generate_training_file(
  md_folder = "./data/0_training_set/0_tokenized_pdfs",  # markown folder
  str_folder = "./data/0_training_set/1_template_json",  # json folder
  output_file = "./data/0_training_set/text2icasa_training_data_1toN.jsonl",  # training data name
  tool_definition = tool_definition,  # the ICASA dict
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
