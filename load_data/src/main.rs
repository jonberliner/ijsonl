use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;
use rayon::prelude::*;
use std::collections::HashMap;
use std::env;

#[derive(Debug, Clone)]
enum DataType {
    Int8,
    Utf8Str,
}

#[derive(Debug, Clone)]
struct ChunkInfo {
    filepath: String,
    start_idx: u64,
    end_idx: u64,
}

#[derive(Debug)]
enum LoadedData {
    Int8(Vec<i8>),
    Utf8Str(Vec<String>),
}

fn load_chunk(file: &Arc<File>, chunk: &ChunkInfo, data_type: &DataType) -> io::Result<LoadedData> {
    let mut buffer = vec![0u8; (chunk.end_idx - chunk.start_idx) as usize];
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(chunk.start_idx))?;
    file.read_exact(&mut buffer)?;

    match data_type {
        DataType::Int8 => Ok(LoadedData::Int8(buffer.into_iter().map(|b| b as i8).collect())),
        DataType::Utf8Str => {
            let s = String::from_utf8(buffer).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(LoadedData::Utf8Str(vec![s]))
        }
    }
}

fn load_arrays(chunks: Vec<ChunkInfo>, data_type: DataType) -> io::Result<LoadedData> {
    let file_map: HashMap<String, Arc<File>> = chunks.iter()
        .map(|chunk| &chunk.filepath)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .map(|filepath| (filepath.clone(), Arc::new(File::open(filepath).unwrap())))
        .collect();

    let loaded_data: Vec<LoadedData> = chunks.into_par_iter()
        .map(|chunk| {
            let file = file_map.get(&chunk.filepath).unwrap();
            load_chunk(file, &chunk, &data_type)
        })
        .collect::<io::Result<Vec<LoadedData>>>()?;

    match data_type {
        DataType::Int8 => {
            let concatenated: Vec<i8> = loaded_data.into_iter()
                .flat_map(|data| if let LoadedData::Int8(vec) = data { vec } else { vec![] })
                .collect();
            Ok(LoadedData::Int8(concatenated))
        },
        DataType::Utf8Str => {
            let concatenated: Vec<String> = loaded_data.into_iter()
                .flat_map(|data| if let LoadedData::Utf8Str(vec) = data { vec } else { vec![] })
                .collect();
            Ok(LoadedData::Utf8Str(concatenated))
        }
    }
}

fn parse_args() -> Vec<(DataType, Vec<ChunkInfo>)> {
    let args: Vec<String> = env::args().collect();
    let mut result = Vec::new();
    let mut i = 1;

    while i < args.len() {
        let data_type = match args[i].as_str() {
            "int8" => DataType::Int8,
            "utf8" => DataType::Utf8Str,
            _ => panic!("Invalid data type: {}", args[i]),
        };
        i += 1;

        let mut chunks = Vec::new();
        while i + 3 <= args.len() && args[i] != "int8" && args[i] != "utf8" {
            chunks.push(ChunkInfo {
                filepath: args[i].clone(),
                start_idx: args[i+1].parse().unwrap(),
                end_idx: args[i+2].parse().unwrap(),
            });
            i += 3;
        }

        result.push((data_type, chunks));
    }

    result
}

fn main() -> io::Result<()> {
    let args = parse_args();

    for (data_type, chunks) in args {
        let result = load_arrays(chunks, data_type)?;
        match result {
            LoadedData::Int8(data) => println!("Loaded {} int8 values", data.len()),
            LoadedData::Utf8Str(data) => {
                println!("Loaded {} strings", data.len());
                for (i, s) in data.iter().enumerate().take(5) {
                    println!("  String {}: {}", i, s);
                }
                if data.len() > 5 {
                    println!("  ...");
                }
            },
        }
    }

    Ok(())
}

