// smartifier2.rs

use clap::{Arg, ArgAction, Command};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::Instant;

// -----------------------------------------------------------------------------
// Helper types/enums
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DataType {
    CSV,
    JSONL,
}

#[derive(Debug)]
struct EdgeCollection {
    file_name: String,
    from_vertex_coll: String,
    to_vertex_coll: String,
    column_renames: Vec<(usize, String)>,
}

// A structure roughly corresponding to the C++ "Translation" struct,
// storing the mapping from "key -> attribute index" and from "attribute -> index".
#[derive(Default)]
struct Translation {
    key_tab: HashMap<String, u32>,
    _att_tab: HashMap<String, u32>,
    smart_attributes: Vec<String>,
    _mem_usage: usize,
}

// -----------------------------------------------------------------------------
// Timing helper
// -----------------------------------------------------------------------------

static mut START_TIME: Option<Instant> = None;

fn elapsed() -> f64 {
    unsafe {
        if let Some(start) = START_TIME {
            let diff = Instant::now().duration_since(start);
            return diff.as_secs_f64();
        }
    }
    0.0
}

// -----------------------------------------------------------------------------
// CSV-related helper functions
// -----------------------------------------------------------------------------

/// Splits a line by a given separator, taking quotes into account.
/// This is a manual approach similar to the C++ version.
fn split(line: &str, sep: char, quo: char) -> Vec<String> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut pos = 0;
    let mut in_quote = false;

    let add = |pos: usize, start: &mut usize, result: &mut Vec<String>| {
        result.push(line[*start..pos].to_string());
        *start = pos + 1;
    };

    while pos < line.len() {
        let c = line.chars().nth(pos).unwrap();
        if !in_quote {
            if c == quo {
                in_quote = true;
                pos += 1;
                continue;
            }
            if c == sep {
                add(pos, &mut start, &mut result);
                pos += 1;
                continue;
            }
            pos += 1;
        } else {
            // in_quote == true
            if c == quo {
                // check if it's a double quote
                if pos + 1 < line.len() && line.chars().nth(pos + 1).unwrap() == quo {
                    // skip both quotes
                    pos += 2;
                    continue;
                }
                in_quote = false;
                pos += 1;
                continue;
            }
            pos += 1;
        }
    }

    // add the last field
    add(pos, &mut start, &mut result);

    result
}

/// Removes surrounding quotes and handles double quotes inside.
fn unquote(s: &str, quo: char) -> String {
    // If there is no quote char at all, return as-is:
    if !s.contains(quo) {
        return s.to_string();
    }

    // We mimic the logic from the C++ version
    let mut res = String::new();
    let mut pos = 0;
    let chars: Vec<char> = s.chars().collect();
    // Find the first quote:
    while pos < chars.len() && chars[pos] != quo {
        pos += 1;
    }
    if pos == chars.len() {
        // no initial quote found
        return s.to_string();
    }
    // skip the quote
    pos += 1;
    let mut in_quote = true;
    while pos < chars.len() {
        if in_quote {
            if chars[pos] == quo {
                if pos + 1 < chars.len() && chars[pos + 1] == quo {
                    // double quote, produce one
                    res.push(quo);
                    pos += 2;
                    continue;
                }
                in_quote = false;
            } else {
                res.push(chars[pos]);
            }
        } else {
            if chars[pos] == quo {
                in_quote = true;
            }
        }
        pos += 1;
    }
    res
}

/// If a string contains the quote character, wrap and double it appropriately.
fn quote_string(s: &str, quo: char) -> String {
    if !s.contains(quo) {
        return s.to_string();
    }
    let mut res = String::new();
    res.push(quo);
    for c in s.chars() {
        if c == quo {
            res.push(quo);
            res.push(quo);
        } else {
            res.push(c);
        }
    }
    res.push(quo);
    res
}

/// Finds the position of a column in a header vector. Returns -1 if not found.
fn find_col_pos(col_headers: &[String], header: &str) -> i32 {
    match col_headers.iter().position(|h| h == header) {
        Some(i) => i as i32,
        None => -1,
    }
}

// -----------------------------------------------------------------------------
// CSV transformations for vertices (mimicking the C++ version)
// -----------------------------------------------------------------------------

fn transform_vertex_csv(
    line: &str,
    count: u64,
    sep: char,
    quo: char,
    ncols: usize,
    smart_attr_pos: i32,
    smart_value_pos: i32,
    smart_index: i32,
    key_pos: i32,
    key_value_pos: i32,
    out: &mut dyn Write,
) {
    let mut parts = split(line, sep, quo);
    // Extend with empty columns if needed
    while parts.len() < ncols {
        parts.push(String::new());
    }
    // Also ensure if smart_attr_pos or key_pos are out-of-range, add empty
    if smart_attr_pos as usize >= parts.len() {
        parts.push(String::new());
    }
    if key_pos as usize >= parts.len() {
        parts.push(String::new());
    }

    // Find the smart graph attribute value
    let att = if smart_value_pos >= 0 && (smart_value_pos as usize) < parts.len() {
        let mut val = unquote(&parts[smart_value_pos as usize], quo);
        if smart_index > 0 && (val.len() as i32) > smart_index {
            val = val[..smart_index as usize].to_string();
        }
        parts[smart_attr_pos as usize] = quote_string(&val, quo);
        val
    } else {
        unquote(&parts[smart_attr_pos as usize], quo)
    };

    // Now handle the key
    let key = if key_value_pos >= 0 && (key_value_pos as usize) < parts.len() {
        unquote(&parts[key_value_pos as usize], quo)
    } else {
        unquote(&parts[key_pos as usize], quo)
    };

    let split_pos = key.find(':');
    if split_pos.is_none() {
        // not yet transformed
        parts[key_pos as usize] = quote_string(&(att.clone() + ":" + &key), quo);
    } else {
        // already has a colon
        let colon_pos = split_pos.unwrap();
        let prefix = &key[..colon_pos];
        if prefix != att {
            eprintln!(
                "Found wrong key w.r.t. smart graph attribute: {} (smart = {}) in line {}",
                key, att, count
            );
            let suffix = &key[colon_pos + 1..];
            parts[key_pos as usize] = quote_string(&(att + ":" + suffix), quo);
        }
    }

    // Write out
    if !parts.is_empty() {
        write!(out, "{}", parts[0]).unwrap();
    }
    for i in 1..parts.len() {
        write!(out, "{}{}", sep, parts[i]).unwrap();
    }
    writeln!(out).unwrap();
}

// -----------------------------------------------------------------------------
// JSONL transformations for vertices
// -----------------------------------------------------------------------------

/// Extract the string value from a JSON field or use a default. This is
/// the simplified Rust version for the C++: `smartToString(...)`.
fn smart_to_string(val: Option<&Value>, smart_default: &str, count: usize) -> String {
    if let Some(v) = val {
        match v {
            Value::String(s) => {
                return s.clone();
            }
            Value::Null => {
                if !smart_default.is_empty() {
                    return smart_default.to_string();
                }
            }
            Value::Bool(b) => {
                eprintln!(
                    "WARNING: Vertex with non-string smart graph attribute (bool) on line {}. Converting to String.",
                    count
                );
                return b.to_string();
            }
            Value::Number(num) => {
                eprintln!(
                    "WARNING: Vertex with non-string smart graph attribute (number) on line {}. Converting to String.",
                    count
                );
                return num.to_string();
            }
            // For arrays/objects, we do not convert:
            other => {
                eprintln!(
                    "ERROR: Found a complex type for the smart graph attribute on line {}: {:?}. Not converting.",
                    count, other
                );
            }
        }
    } else {
        // no such field => use default
        if !smart_default.is_empty() {
            return smart_default.to_string();
        }
    }
    "".to_string()
}

/// Transform a single JSON line for a vertex, adjusting `_key` and the
/// specified "smart graph attribute".
fn transform_vertex_jsonl(
    line: &str,
    count: usize,
    smart_attr: &str,
    smart_value: &str,
    smart_index: i32,
    smart_default: &str,
    write_key: bool,
    key_value: &str,
    out: &mut dyn Write,
) {
    // Parse JSON
    let parsed: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("JSON parse error on line {}: {}", count, e);
            return;
        }
    };

    // We expect an object for each line
    let obj = match parsed {
        Value::Object(m) => m,
        _ => {
            eprintln!(
                "Expected an object in JSON line {}, found something else. Skipping.",
                count
            );
            return;
        }
    };

    // Derive the smart graph attribute
    let att_val = if !smart_value.is_empty() {
        smart_to_string(obj.get(smart_value), smart_default, count)
    } else {
        smart_to_string(obj.get(smart_attr), smart_default, count)
    };
    let mut final_att_val = att_val.clone();
    if smart_index > 0 && (final_att_val.len() as i32) > smart_index {
        final_att_val = final_att_val[..smart_index as usize].to_string();
    }

    // Figure out the new _key
    let key_slice = if !key_value.is_empty() {
        obj.get(key_value)
    } else {
        obj.get("_key")
    };
    let mut new_key = String::new();
    if let Some(Value::String(key_str)) = key_slice {
        let split_pos = key_str.find(':');
        if let Some(pos) = split_pos {
            // Already has a colon
            if &key_str[..pos] != final_att_val {
                eprintln!(
                    "_key is already smart, but with the wrong prefix on line {}: {} (smart = {})",
                    count, key_str, final_att_val
                );
            }
            new_key = key_str.clone();
        } else {
            // Not yet transformed
            if !final_att_val.is_empty() {
                new_key = final_att_val.clone() + ":" + key_str;
            } else {
                new_key = key_str.clone();
            }
        }
    }

    // Write out a new object
    // The C++ code prints `_key` and the smart attribute first, then the rest.
    let mut new_obj = Map::new();

    if write_key || !new_key.is_empty() {
        new_obj.insert("_key".to_string(), Value::String(new_key));
    }
    new_obj.insert(smart_attr.to_string(), Value::String(final_att_val));

    // Then copy over all other fields that are not `_key` / `smart_attr`
    let reserved = vec!["_key".to_string(), smart_attr.to_string()];
    for (k, v) in obj.into_iter() {
        if !reserved.contains(&k) {
            new_obj.insert(k, v);
        }
    }

    // Serialize to JSON
    let output = Value::Object(new_obj);
    if let Ok(line_out) = serde_json::to_string(&output) {
        writeln!(out, "{}", line_out).unwrap();
    } else {
        eprintln!(
            "Failed to serialize transformed JSON for line {}. Skipping.",
            count
        );
    }
}

// -----------------------------------------------------------------------------
// Example: a minimal "main" with clap for argument parsing
// -----------------------------------------------------------------------------

fn main() {
    unsafe {
        START_TIME = Some(Instant::now());
    }

    let matches = Command::new("smartifier3")
        .version("3.0")
        .about("Transform graph data into smart graph format (Rust version)")
        .subcommand_required(true)
        .subcommand(
            Command::new("vertices")
                .about("Transform vertices into smart graph format")
                .arg(
                    Arg::new("input")
                        .long("input")
                        .short('i')
                        .num_args(1)
                        .required(true)
                        .help("Input file (CSV or JSONL)"),
                )
                .arg(
                    Arg::new("output")
                        .long("output")
                        .short('o')
                        .num_args(1)
                        .required(true)
                        .help("Output file (CSV or JSONL)"),
                )
                .arg(
                    Arg::new("smart-graph-attribute")
                        .long("smart-graph-attribute")
                        .num_args(1)
                        .default_value("smart_id")
                        .help("Name of the smart graph attribute"),
                )
                .arg(
                    Arg::new("type")
                        .long("type")
                        .num_args(1)
                        .default_value("csv")
                        .help("Input data type: csv or jsonl"),
                )
                .arg(
                    Arg::new("separator")
                        .long("separator")
                        .default_value(",")
                        .help("Column separator for CSV"),
                )
                .arg(
                    Arg::new("quote-char")
                        .long("quote-char")
                        .default_value("\"")
                        .help("Quote character for CSV"),
                )
                .arg(
                    Arg::new("write-key")
                        .long("write-key")
                        .action(ArgAction::SetTrue)
                        .help("If present, the `_key` attribute will be re-written"),
                )
                .arg(
                    Arg::new("smart-value")
                        .long("smart-value")
                        .num_args(1)
                        .help("Attribute/column used to build the smart graph attribute"),
                )
                .arg(
                    Arg::new("smart-index")
                        .long("smart-index")
                        .num_args(1)
                        .help("If given, only this many characters are taken from the smart value"),
                )
                .arg(
                    Arg::new("smart-default")
                        .long("smart-default")
                        .num_args(1)
                        .help("Default value for smart graph attribute if not present (JSONL only)"),
                )
                .arg(
                    Arg::new("key-value")
                        .long("key-value")
                        .num_args(1)
                        .help("Column/attribute name from which to get the value for `_key` suffix"),
                )
        )
        .subcommand(
            Command::new("edges")
                .about("Transform edges into smart graph format")
                .arg(
                    Arg::new("type")
                        .long("type")
                        .num_args(1)
                        .default_value("csv")
                        .help("Input data type: csv or jsonl"),
                )
                .arg(
                    Arg::new("separator")
                        .long("separator")
                        .default_value(",")
                        .help("Column separator for CSV"),
                )
                .arg(
                    Arg::new("quote-char")
                        .long("quote-char")
                        .default_value("\"")
                        .help("Quote character for CSV"),
                )
                .arg(
                    Arg::new("edges")
                        .long("edges")
                        .num_args(..)
                        .required(true)
                        .help("One or more edge specifications: <edgefile>:<fromColl>:<toColl>[:<colIndex>:<newName> ...]"),
                )
                .arg(
                    Arg::new("smart-index")
                        .long("smart-index")
                        .num_args(1)
                        .help("If >0, take this many chars from the key for smart attribute"),
                )
        )
        .get_matches();

    match matches.subcommand() {
        Some(("vertices", sub_m)) => {
            let input = sub_m.get_one::<String>("input").unwrap().clone();
            let output = sub_m.get_one::<String>("output").unwrap().clone();
            let smart_attr = sub_m
                .get_one::<String>("smart-graph-attribute")
                .unwrap()
                .clone();
            let data_type_str = sub_m.get_one::<String>("type").unwrap().to_lowercase();
            let data_type = if data_type_str == "jsonl" {
                DataType::JSONL
            } else {
                DataType::CSV
            };
            let sep = sub_m
                .get_one::<String>("separator")
                .unwrap()
                .chars()
                .next()
                .unwrap();
            let quo = sub_m
                .get_one::<String>("quote-char")
                .unwrap()
                .chars()
                .next()
                .unwrap();
            let write_key = sub_m.get_flag("write-key");
            let smart_value = sub_m
                .get_one::<String>("smart-value")
                .unwrap_or(&"".to_string())
                .clone();
            let smart_index_str = sub_m
                .get_one::<String>("smart-index")
                .unwrap_or(&"".to_string())
                .clone();
            let smart_index: i32 = if smart_index_str.is_empty() {
                -1
            } else {
                smart_index_str.parse().unwrap_or(-1)
            };
            let smart_default = sub_m
                .get_one::<String>("smart-default")
                .unwrap_or(&"".to_string())
                .clone();
            let key_value = sub_m
                .get_one::<String>("key-value")
                .unwrap_or(&"".to_string())
                .clone();

            std::process::exit(do_vertices(
                &input,
                &output,
                &smart_attr,
                data_type,
                sep,
                quo,
                write_key,
                &smart_value,
                smart_index,
                &smart_default,
                &key_value,
            ));
        }
        Some(("edges", sub_m)) => {
            let data_type_str = sub_m.get_one::<String>("type").unwrap().to_lowercase();
            let data_type = if data_type_str == "jsonl" {
                DataType::JSONL
            } else {
                DataType::CSV
            };
            let sep = sub_m
                .get_one::<String>("separator")
                .unwrap()
                .chars()
                .next()
                .unwrap();
            let quo = sub_m
                .get_one::<String>("quote-char")
                .unwrap()
                .chars()
                .next()
                .unwrap();
            let edges_list = sub_m.get_many::<String>("edges").unwrap();
            let smart_index_str = sub_m
                .get_one::<String>("smart-index")
                .unwrap_or(&"".to_string())
                .clone();
            let smart_index: i32 = if smart_index_str.is_empty() {
                -1
            } else {
                smart_index_str.parse().unwrap_or(-1)
            };

            let edges_list: Vec<String> = edges_list.map(|x| x.clone()).collect();
            let edge_collections = parse_edge_collections(edges_list);
            std::process::exit(do_edges(
                data_type,
                sep,
                quo,
                &edge_collections,
                smart_index,
            ));
        }
        _ => {
            eprintln!("No valid subcommand given.");
            std::process::exit(-1);
        }
    }
}

// -----------------------------
// Implementation of do_vertices
// -----------------------------

fn do_vertices(
    input_file: &str,
    output_file: &str,
    smart_attr: &str,
    data_type: DataType,
    sep: char,
    quo: char,
    write_key: bool,
    smart_value: &str,
    smart_index: i32,
    smart_default: &str,
    key_value: &str,
) -> i32 {
    // open input
    let input = match File::open(input_file) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Cannot open input file {}: {}", input_file, e);
            return 1;
        }
    };
    let reader = BufReader::new(input);

    // open output
    let output = match File::create(output_file) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Cannot open output file {}: {}", output_file, e);
            return 2;
        }
    };
    let mut writer = BufWriter::new(output);

    match data_type {
        DataType::CSV => {
            // We read the first line as the header:
            let mut lines = reader.lines();
            let Some(Ok(header_line)) = lines.next() else {
                eprintln!("Could not read header line in vertex file {}", input_file);
                return 3;
            };

            let mut col_headers = split(&header_line, sep, quo)
                .into_iter()
                .map(|s| unquote(&s, quo))
                .collect::<Vec<String>>();
            let mut ncols = col_headers.len();

            // Try to find or create the column for the smart attribute
            let mut smart_attr_pos = find_col_pos(&col_headers, smart_attr);
            if smart_attr_pos < 0 {
                smart_attr_pos = ncols as i32;
                col_headers.push(smart_attr.to_string());
                ncols += 1;
            }

            // If we have a separate smart_value column, see if it exists
            let mut smart_value_pos = -1;
            if !smart_value.is_empty() {
                smart_value_pos = find_col_pos(&col_headers, smart_value);
                if smart_value_pos < 0 {
                    eprintln!(
                        "Warning: could not find the smart value column {}. Ignoring...",
                        smart_value
                    );
                }
            }

            // For _key
            let mut key_pos = find_col_pos(&col_headers, "_key");
            if key_pos < 0 && write_key {
                key_pos = ncols as i32;
                col_headers.push("_key".to_string());
                ncols += 1;
            }

            let mut key_value_pos = -1;
            if !key_value.is_empty() {
                key_value_pos = find_col_pos(&col_headers, key_value);
                if key_value_pos < 0 && write_key {
                    eprintln!(
                        "Warning: could not find column {} for key value. Ignoring...",
                        key_value
                    );
                }
            }

            // Write out the new header
            if !col_headers.is_empty() {
                write!(writer, "{}", quote_string(&col_headers[0], quo)).unwrap();
            }
            for c in &col_headers[1..] {
                write!(writer, "{}", sep).unwrap();
                write!(writer, "{}", quote_string(c, quo)).unwrap();
            }
            writeln!(writer).unwrap();

            let mut count: u64 = 1;
            for line_result in lines {
                let Ok(line_str) = line_result else {
                    continue; // skip ill-formed lines
                };
                transform_vertex_csv(
                    &line_str,
                    count + 1,
                    sep,
                    quo,
                    ncols,
                    smart_attr_pos,
                    smart_value_pos,
                    smart_index,
                    key_pos,
                    key_value_pos,
                    &mut writer,
                );
                count += 1;
                if count % 1_000_000 == 0 {
                    println!("{:.3} Have transformed {} vertices.", elapsed(), count);
                }
            }
        }
        DataType::JSONL => {
            let mut count = 1;
            for line_result in reader.lines() {
                let Ok(line_str) = line_result else {
                    continue;
                };
                transform_vertex_jsonl(
                    &line_str,
                    count,
                    smart_attr,
                    smart_value,
                    smart_index,
                    smart_default,
                    write_key,
                    key_value,
                    &mut writer,
                );
                count += 1;
                if count % 1_000_000 == 0 {
                    println!("{:.3} Have transformed {} vertices.", elapsed(), count);
                }
            }
        }
    }

    // Make sure we flush and close properly
    if let Err(e) = writer.flush() {
        eprintln!("Error flushing output file {}: {}", output_file, e);
        return 4;
    }
    0
}

// -----------------------------------------------------------------------------
// do_edges: a greatly simplified version that handles only the main aspects
// -----------------------------------------------------------------------------

fn parse_edge_collections(edges_list: Vec<String>) -> Vec<EdgeCollection> {
    let mut collections = Vec::new();

    for e in edges_list {
        // Format: <file>:<fromColl>:<toColl>[:<colIndex>:<newName> ...]
        // We'll manually parse up to the third colon, then parse renames.
        let parts: Vec<_> = e.split(':').map(|s| s.to_string()).collect();
        if parts.len() < 3 {
            eprintln!("Invalid format for edge spec '{}'. Skipping.", e);
            continue;
        }

        let file_name = parts[0].clone();
        let from_vertex_coll = parts[1].clone();
        let to_vertex_coll = parts[2].clone();

        // The rest might be rename specs in pairs: (colIndex, newName)
        let mut renames = Vec::new();
        let mut idx = 3;
        while idx + 1 < parts.len() {
            let col_index_str = &parts[idx];
            let new_name = &parts[idx + 1];
            if let Ok(col_index) = col_index_str.parse::<usize>() {
                renames.push((col_index, new_name.clone()));
            }
            idx += 2;
        }

        collections.push(EdgeCollection {
            file_name,
            from_vertex_coll,
            to_vertex_coll,
            column_renames: renames,
        });
    }

    collections
}

/// Transforms edges in CSV. We do a minimal version that ensures `_from` and `_to`
/// get turned into "att:KEY" if not already present, etc. This is simplified
/// compared to the original C++ version.
fn transform_edges_csv(
    edge_coll: &EdgeCollection,
    sep: char,
    quo: char,
    smart_index: i32,
    translation: &Translation,
) -> i32 {
    println!(
        "{:.3} Transforming edges in {}",
        elapsed(),
        edge_coll.file_name
    );

    // read original
    let in_path = Path::new(&edge_coll.file_name);
    let edge_file_name = edge_coll.file_name.clone() + ".out";
    let out_path = Path::new(&edge_file_name);
    let input = match File::open(in_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Cannot open edge file {}: {}", &edge_coll.file_name, e);
            return 1;
        }
    };
    let reader = BufReader::new(input);
    let output = match File::create(&out_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "Cannot create temp edge file {}.out: {}",
                &edge_coll.file_name, e
            );
            return 2;
        }
    };
    let mut writer = BufWriter::new(output);

    let mut lines = reader.lines();
    let Some(Ok(header_line)) = lines.next() else {
        eprintln!("Empty or invalid edge file {}", &edge_coll.file_name);
        return 3;
    };
    let mut col_headers = split(&header_line, sep, quo)
        .into_iter()
        .map(|s| unquote(&s, quo))
        .collect::<Vec<String>>();

    // rename columns if needed
    for (col_idx, new_name) in &edge_coll.column_renames {
        if *col_idx < col_headers.len() {
            col_headers[*col_idx] = new_name.clone();
        }
    }

    // write out the new header
    if !col_headers.is_empty() {
        write!(writer, "{}", quote_string(&col_headers[0], quo)).unwrap();
    }
    for c in &col_headers[1..] {
        write!(writer, "{}{}", sep, quote_string(c, quo)).unwrap();
    }
    writeln!(writer).unwrap();

    // try to find _from, _to, _key
    let from_pos = find_col_pos(&col_headers, "_from");
    let to_pos = find_col_pos(&col_headers, "_to");
    let key_pos = find_col_pos(&col_headers, "_key");

    if from_pos < 0 || to_pos < 0 {
        eprintln!(
            "Did not find _from or _to field in {}, skipping transformations.",
            edge_coll.file_name
        );
        return 4;
    }

    let mut count = 0usize;
    for line_result in lines {
        let Ok(line_str) = line_result else {
            continue;
        };
        let mut parts = split(&line_str, sep, quo);
        while parts.len() < col_headers.len() {
            parts.push(String::new());
        }

        // We'll define an inline closure to fix either _from or _to
        // in a simplified manner. The original code tries to do a
        // "translation" from a big table, or do prefix from smart_index.
        // We'll do a partial approach here.

        let fix_vertex = |pos: usize, default_coll: &str, parts: &mut [String]| -> String {
            let unquoted = unquote(&parts[pos], quo);
            if unquoted.contains('/') {
                // see if we already have a colon
                if unquoted.find(':').is_some() {
                    // already transformed
                    parts[pos] = quote_string(&unquoted, quo);
                    let slashpos = unquoted.find('/').unwrap_or(0);
                    let after_slash = &unquoted[slashpos + 1..];
                    if let Some(colpos) = after_slash.find(':') {
                        // get the part between slash and colon
                        return after_slash[..colpos].to_string();
                    }
                    return "".to_string();
                } else {
                    // we do default approach
                    let slashpos = unquoted.find('/').unwrap();
                    let key_after_slash = &unquoted[slashpos + 1..];
                    if smart_index > 0 && key_after_slash.len() as i32 > smart_index {
                        let att = &key_after_slash[..(smart_index as usize)];
                        let new_value =
                            format!("{}/{}:{}", &unquoted[..slashpos], att, &key_after_slash);
                        parts[pos] = quote_string(&new_value, quo);
                        return att.to_string();
                    } else {
                        // do the translation approach
                        let mut found_smart = "".to_string();
                        let full_key = unquoted;
                        if let Some(&att_idx) = translation.key_tab.get(&full_key) {
                            let att = &translation.smart_attributes[att_idx as usize];
                            let after_s = &full_key[slashpos + 1..];
                            let new_val = format!("{}/{}:{}", &full_key[..slashpos], att, after_s);
                            parts[pos] = quote_string(&new_val, quo);
                            found_smart = att.to_string();
                        } else {
                            // not found => keep as is
                            parts[pos] = quote_string(&full_key, quo);
                        }
                        return found_smart;
                    }
                }
            } else {
                // doesn't contain '/', so we do the default
                let new_value = format!("{}/{}", default_coll, unquoted);
                // now do we do the colon?
                if smart_index > 0 && unquoted.len() as i32 > smart_index {
                    let att = &unquoted[..(smart_index as usize)];
                    let final_val = format!("{}/{}:{}", default_coll, att, &unquoted);
                    parts[pos] = quote_string(&final_val, quo);
                    return att.to_string();
                } else {
                    // see if it is in translation
                    let full_key = format!("{}/{}", default_coll, unquoted);
                    if let Some(&att_idx) = translation.key_tab.get(&full_key) {
                        let att = &translation.smart_attributes[att_idx as usize];
                        let just_key = unquoted;
                        let final_val = format!("{}/{}:{}", default_coll, att, just_key);
                        parts[pos] = quote_string(&final_val, quo);
                        return att.to_string();
                    } else {
                        parts[pos] = quote_string(&new_value, quo);
                        return "".to_string();
                    }
                }
            }
        };

        let from_attr = fix_vertex(from_pos as usize, &edge_coll.from_vertex_coll, &mut parts);
        let to_attr = fix_vertex(to_pos as usize, &edge_coll.to_vertex_coll, &mut parts);

        // If _key is present and from/to are valid, then we might do a triple prefix
        if key_pos >= 0 && !from_attr.is_empty() && !to_attr.is_empty() {
            let kpos = key_pos as usize;
            let unquoted_key = unquote(&parts[kpos], quo);
            if !unquoted_key.contains(':') {
                let new_key = format!("{}:{}:{}", from_attr, unquoted_key, to_attr);
                parts[kpos] = quote_string(&new_key, quo);
            }
        }

        // rewrite the line
        if !parts.is_empty() {
            write!(writer, "{}", parts[0]).unwrap();
        }
        for i in 1..parts.len() {
            write!(writer, "{}{}", sep, parts[i]).unwrap();
        }
        writeln!(writer).unwrap();

        count += 1;
        if count % 1_000_000 == 0 {
            println!(
                "{:.3} Have transformed {} edges in {} ...",
                elapsed(),
                count,
                edge_coll.file_name
            );
        }
    }

    // flush/close
    if let Err(e) = writer.flush() {
        eprintln!(
            "Error flushing edge temp file {}.out: {}",
            edge_coll.file_name, e
        );
        return 5;
    }

    // rename old file -> .bak or remove, rename new file -> old
    std::fs::remove_file(&edge_coll.file_name).ok();
    std::fs::rename(&out_path, &in_path).ok();

    println!(
        "{:.3} Done transforming edges in {}",
        elapsed(),
        edge_coll.file_name
    );
    0
}

/// Transform edges in JSONL in a similar manner
fn transform_edges_jsonl(
    edge_coll: &EdgeCollection,
    smart_index: i32,
    translation: &Translation,
) -> i32 {
    println!(
        "{:.3} Transforming JSON edges in {}",
        elapsed(),
        edge_coll.file_name
    );
    let in_path = Path::new(&edge_coll.file_name);
    let edge_file_name_out = edge_coll.file_name.clone() + ".out";
    let out_path = Path::new(&edge_file_name_out);
    let input = match File::open(in_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Cannot open edge file {}: {}", &edge_coll.file_name, e);
            return 1;
        }
    };
    let reader = BufReader::new(input);
    let output = match File::create(&out_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "Cannot create temp edge file {}.out: {}",
                &edge_coll.file_name, e
            );
            return 2;
        }
    };
    let mut writer = BufWriter::new(output);

    let mut count = 0usize;
    for line_result in reader.lines() {
        let Ok(line_str) = line_result else { continue };
        let parsed: Value = match serde_json::from_str(&line_str) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "JSON parse error in file {}, line {}: {}",
                    edge_coll.file_name, count, e
                );
                continue;
            }
        };

        // We expect an object
        let mut obj = match parsed {
            Value::Object(m) => m,
            _ => {
                eprintln!(
                    "Non-object line in JSON edges file {}, line {}. Skipping.",
                    edge_coll.file_name, count
                );
                continue;
            }
        };

        // fix from/to
        let (found_from, new_from, from_attr) = fix_json_vertex(
            &mut obj,
            "_from",
            &edge_coll.from_vertex_coll,
            smart_index,
            translation,
        );
        let (found_to, new_to, to_attr) = fix_json_vertex(
            &mut obj,
            "_to",
            &edge_coll.to_vertex_coll,
            smart_index,
            translation,
        );

        let mut new_key = None;
        if let (true, Some(fa), true, Some(ta)) =
            (found_from, from_attr.clone(), found_to, to_attr.clone())
        {
            // then we see if _key is present
            if let Some(Value::String(k)) = obj.get("_key") {
                if !k.contains(':') {
                    let triple = format!("{}:{}:{}", fa, k, ta);
                    new_key = Some(triple);
                }
            }
        }

        // build a new map to preserve order similar to the transform
        let mut new_map = Map::new();
        if let Some(k) = new_key {
            new_map.insert("_key".to_string(), Value::String(k));
        } else if let Some(v) = obj.get("_key") {
            new_map.insert("_key".to_string(), v.clone());
        }
        if let Some(nf) = new_from {
            new_map.insert("_from".to_string(), Value::String(nf));
        } else if let Some(v) = obj.get("_from") {
            new_map.insert("_from".to_string(), v.clone());
        }
        if let Some(nt) = new_to {
            new_map.insert("_to".to_string(), Value::String(nt));
        } else if let Some(v) = obj.get("_to") {
            new_map.insert("_to".to_string(), v.clone());
        }

        // copy the rest
        for (k, v) in obj.into_iter() {
            if k != "_key" && k != "_from" && k != "_to" {
                new_map.insert(k, v);
            }
        }

        if let Ok(line_out) = serde_json::to_string(&Value::Object(new_map)) {
            writeln!(writer, "{}", line_out).unwrap();
        }

        count += 1;
        if count % 1_000_000 == 0 {
            println!(
                "{:.3} Have transformed {} edges in {} ...",
                elapsed(),
                count,
                edge_coll.file_name
            );
        }
    }

    if let Err(e) = writer.flush() {
        eprintln!(
            "Error flushing edge temp file {}.out: {}",
            edge_coll.file_name, e
        );
        return 5;
    }
    std::fs::remove_file(&edge_coll.file_name).ok();
    std::fs::rename(&out_path, &in_path).ok();
    println!(
        "{:.3} Done transforming edges in {}",
        elapsed(),
        edge_coll.file_name
    );
    0
}

/// Helper to fix "_from" or "_to" in JSON
fn fix_json_vertex(
    obj: &mut Map<String, Value>,
    field: &str,
    default_coll: &str,
    smart_index: i32,
    translation: &Translation,
) -> (bool, Option<String>, Option<String>) {
    let val_opt = obj.get(field);
    if val_opt.is_none() {
        return (false, None, None);
    }
    let val = val_opt.unwrap();
    if !val.is_string() {
        eprintln!("{} is not a string, skipping transformation.", field);
        return (true, None, None);
    }
    let old_val = val.as_str().unwrap().to_string();

    // Return the chosen "smart" portion as well
    let (new_val, att) = if old_val.contains('/') {
        // see if there's already a colon
        if old_val.find(':').is_some() {
            // already transformed
            (old_val.clone(), None)
        } else {
            // check smart_index
            let slashpos = old_val.find('/').unwrap_or(0);
            let after_slash = &old_val[slashpos + 1..];
            if smart_index > 0 && (after_slash.len() as i32) > smart_index {
                let prefix = &after_slash[..smart_index as usize];
                let new_str = format!("{}/{}:{}", &old_val[..slashpos], prefix, &after_slash);
                (new_str, Some(prefix.to_string()))
            } else {
                // do we have a translation?
                if let Some(&idx) = translation.key_tab.get(&old_val) {
                    let att = translation.smart_attributes[idx as usize].clone();
                    let key_after_slash = &old_val[slashpos + 1..];
                    let new_str = format!("{}/{}:{}", &old_val[..slashpos], att, key_after_slash);
                    (new_str, Some(att))
                } else {
                    // keep as is
                    (old_val.clone(), None)
                }
            }
        }
    } else {
        // no '/', so add default_coll
        let full_key = format!("{}/{}", default_coll, old_val);
        if smart_index > 0 && old_val.len() as i32 > smart_index {
            let prefix = &old_val[..smart_index as usize];
            let new_str = format!("{}/{}:{}", default_coll, prefix, &old_val);
            (new_str, Some(prefix.to_string()))
        } else {
            if let Some(&idx) = translation.key_tab.get(&full_key) {
                let att = translation.smart_attributes[idx as usize].clone();
                let after_slash = old_val;
                let new_str = format!("{}/{}:{}", default_coll, att, after_slash);
                (new_str, Some(att))
            } else {
                (full_key, None)
            }
        }
    };

    (true, Some(new_val), att)
}

fn do_edges(
    data_type: DataType,
    sep: char,
    quo: char,
    edge_collections: &[EdgeCollection],
    smart_index: i32,
) -> i32 {
    // In the original C++ code, there's logic about reading vertex files,
    // building a "Translation" table, then processing edges in chunks.
    // Here, for simplicity, we will assume no vertex data or we have
    // no new lines to read for them. We'll provide an empty or default
    // translation.

    let translation = Translation::default();
    // If you had logic to fill `translation` from vertex files, you'd do it here.

    for coll in edge_collections {
        let res = match data_type {
            DataType::CSV => transform_edges_csv(coll, sep, quo, smart_index, &translation),
            DataType::JSONL => transform_edges_jsonl(coll, smart_index, &translation),
        };
        if res != 0 {
            return res;
        }
    }

    0
}
