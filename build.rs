use std::env;
use std::fs;
use std::path::Path;

fn preprocess_spec(spec: &mut serde_json::Value) {
  let schemas = match spec
    .get_mut("components")
    .and_then(|c| c.get_mut("schemas"))
  {
    Some(s) => s,
    None => return,
  };

  let schemas_obj = match schemas.as_object_mut() {
    Some(o) => o,
    None => return,
  };

  // Fix nullable enums: remove `null` from enum value arrays and add `nullable: true`.
  // The Alpaca spec includes `null` as an enum variant (e.g. ExchangeForPosition)
  // which causes progenitor/typify to generate recursive wrapper types.
  let keys: Vec<String> = schemas_obj.keys().cloned().collect();
  for key in &keys {
    if let Some(schema) = schemas_obj.get_mut(key) {
      fix_nullable_enums(schema);
    }
  }

  // Rename schemas that collide when converted to PascalCase.
  // For example, `Clock` (v2) and `clock` (v3) both become `Clock` in Rust.
  let case_renames = find_case_collisions(schemas_obj);
  for (old_name, new_name) in &case_renames {
    if let Some(val) = schemas_obj.remove(old_name) {
      schemas_obj.insert(new_name.clone(), val);
    }
  }

  if !case_renames.is_empty() {
    fix_refs(spec, &case_renames);
  }
}

fn fix_nullable_enums(value: &mut serde_json::Value) {
  match value {
    serde_json::Value::Object(obj) => {
      if let Some(enum_val) = obj.get_mut("enum") {
        if let Some(arr) = enum_val.as_array_mut() {
          let had_null = arr.iter().any(|v| v.is_null());
          if had_null {
            arr.retain(|v| !v.is_null());
            obj.insert("nullable".to_string(), serde_json::Value::Bool(true));
          }
        }
      }
      for val in obj.values_mut() {
        fix_nullable_enums(val);
      }
    },
    serde_json::Value::Array(arr) => {
      for val in arr {
        fix_nullable_enums(val);
      }
    },
    _ => {},
  }
}

fn find_case_collisions(
  schemas: &serde_json::Map<String, serde_json::Value>,
) -> Vec<(String, String)> {
  use std::collections::HashMap;

  let mut seen: HashMap<String, Vec<String>> = HashMap::new();
  for key in schemas.keys() {
    let normalized = to_pascal_case(key);
    seen.entry(normalized).or_default().push(key.clone());
  }

  let mut renames = Vec::new();
  for (_normalized, names) in &seen {
    if names.len() > 1 {
      // Keep the first name, rename subsequent ones with a `_v3` suffix
      // (since the lowercase variant is typically the newer v3 schema).
      for name in &names[1..] {
        let new_name = format!("{}_v3", name);
        renames.push((name.clone(), new_name));
      }
    }
  }
  renames
}

fn to_pascal_case(s: &str) -> String {
  let mut result = String::new();
  let mut capitalize_next = true;
  for ch in s.chars() {
    if ch == '_' || ch == '-' {
      capitalize_next = true;
    } else if capitalize_next {
      result.push(ch.to_ascii_uppercase());
      capitalize_next = false;
    } else {
      result.push(ch);
    }
  }
  result
}

fn fix_refs(value: &mut serde_json::Value, renames: &[(String, String)]) {
  match value {
    serde_json::Value::Object(obj) => {
      if let Some(ref_val) = obj.get_mut("$ref") {
        if let Some(s) = ref_val.as_str() {
          for (old_name, new_name) in renames {
            let old_ref = format!("#/components/schemas/{}", old_name);
            if s == old_ref {
              *ref_val = serde_json::Value::String(format!("#/components/schemas/{}", new_name));
              break;
            }
          }
        }
      }
      for val in obj.values_mut() {
        fix_refs(val, renames);
      }
    },
    serde_json::Value::Array(arr) => {
      for val in arr {
        fix_refs(val, renames);
      }
    },
    _ => {},
  }
}

fn generate_api(spec_path: &str, output_name: &str, out_dir: &str) {
  let spec_str =
    fs::read_to_string(spec_path).unwrap_or_else(|e| panic!("Failed to read {spec_path}: {e}"));
  let mut spec_value: serde_json::Value =
    serde_json::from_str(&spec_str).unwrap_or_else(|e| panic!("Failed to parse {spec_path}: {e}"));

  preprocess_spec(&mut spec_value);

  let spec: openapiv3::OpenAPI = serde_json::from_value(spec_value)
    .unwrap_or_else(|e| panic!("Failed to convert preprocessed {spec_path} to OpenAPI: {e}"));

  let mut generator = progenitor::Generator::default();
  match generator.generate_tokens(&spec) {
    Ok(tokens) => {
      let ast = syn::parse2(tokens).expect("Failed to parse generated tokens");
      let formatted = prettyplease::unparse(&ast);
      let out_path = Path::new(out_dir).join(output_name);
      fs::write(&out_path, &formatted).unwrap();
      eprintln!(
        "cargo:warning={output_name} generated successfully: {} bytes",
        formatted.len()
      );
    },
    Err(e) => {
      panic!("{output_name} generation FAILED: {e}");
    },
  }

  println!("cargo:rerun-if-changed={spec_path}");
}

fn main() {
  let out_dir = env::var("OUT_DIR").unwrap();
  generate_api("specs/trading-api.json", "trading_api.rs", &out_dir);
  generate_api("specs/market-data-api.json", "market_data_api.rs", &out_dir);
}
