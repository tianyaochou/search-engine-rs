use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::vec::Vec;

use walkdir::WalkDir;

use crossbeam;
use rayon::iter::ParallelBridge;
use rayon::prelude::*;

use mailparse::*;

// use rust_stemmers::Stemmer;
use rust_tokenizers::tokenizer::{BertTokenizer, Tokenizer};
use rust_tokenizers::vocab::{BertVocab, Vocab};
use stopwords::{Language, Spark, Stopwords};

use bincode;
use bincode::config::Options;
use sled;

pub static BERT_VOCAB_PATH: &str = "./bert.txt";
pub static DB_PATH: &str = "output/db";

pub type TokenID = i64;
pub type FileID = u64;

fn main() -> io::Result<()> {
    let process_dir = match std::env::args().nth(1) {
        Some(a) => a,
        None => "".to_string(),
    };
    // init nlp tools
    let vocab_path = BERT_VOCAB_PATH;
    let vocab = BertVocab::from_file(&vocab_path).expect("Failed to load vocab");
    let tokenizer = BertTokenizer::from_existing_vocab(vocab, true, false);
    // let stemmer = Stemmer::create(rust_stemmers::Algorithm::English);
    let stops: HashSet<_> = Spark::stopwords(Language::English)
        .unwrap()
        .iter()
        .map(|&x| x.to_string())
        .collect();

    // init db
    let db: sled::Db = sled::open(DB_PATH).unwrap();
    // init serialization
    let bincode_config = bincode::options().with_big_endian();

    // load files
    let (read_tx, read_rx) = crossbeam::channel::bounded(1024);
    let files_tree = db.open_tree("files").unwrap();
    let db1 = db.clone();
    std::thread::spawn(move || {
        for entry in WalkDir::new(process_dir) {
            let mut raw_file = Vec::new();
            let entry = entry.unwrap();
            let path = entry.path();
            if !path.is_dir()
                && !path
                    .file_name()
                    .expect("file name is .. or /")
                    .to_string_lossy()
                    .starts_with(".")
            {
                let mut f = File::open(path).unwrap();
                f.read_to_end(&mut raw_file).unwrap();
                let path_string = path.to_string_lossy();
                let file_id = db1.generate_id().unwrap();
                files_tree
                    .insert(
                        bincode_config.serialize(&file_id).unwrap().as_slice(),
                        path_string.as_bytes(),
                    )
                    .unwrap();
                let _ = read_tx.send((file_id, raw_file));
            };
        }
    });

    // parallel tokenizing
    let (token_tx, token_rx) = crossbeam::channel::bounded(1024);
    std::thread::spawn(move || {
        read_rx.clone().into_iter().par_bridge().for_each(|x| {
            let (path, raw_file) = x;
            match mailparse::parse_mail(&raw_file) {
                Ok(email) => {
                    let subject = email
                        .get_headers()
                        .get_first_header("Subject")
                        .unwrap()
                        .get_value();
                    let content = email.get_body().unwrap();
                    let mut tokens = tokenizer.tokenize(subject);
                    let content_tokens = tokenizer.tokenize(content);
                    tokens.extend(content_tokens);
                    let tokens: Vec<String> = tokens
                        .into_iter()
                        // TODO: .map(|s| stemmer.stem(&s).to_string())
                        .filter(|x| !stops.contains(x) && x.len() > 1)
                        .map(|s| s.to_string())
                        .collect();
                    let token_ids = tokenizer.convert_tokens_to_ids(tokens);
                    let _ = token_tx.send((path, token_ids));
                }
                Err(_) => {
                    let _ = token_tx.send((path, [].to_vec()));
                }
            };
        });
    });

    // count token frequencies
    let (count_tx, count_rx) = crossbeam::channel::bounded(1024);
    let tf_df_tree = db.open_tree("tf_df").unwrap();
    let tf_df_tree1 = tf_df_tree.clone();
    std::thread::spawn(move || {
        for (file_id, mut token_ids) in token_rx {
            let mut token_frequncies: HashMap<TokenID, u64> = HashMap::new();
            for token_id in &token_ids {
                // count token freuencies
                match token_frequncies.get_mut(&token_id) {
                    Some(c) => *c += 1,
                    None => {
                        token_frequncies.insert(*token_id, 1);
                    }
                }
            }
            for (token, tf) in &token_frequncies {
                tf_df_tree
                    .insert(
                        bincode_config.serialize(&(file_id, *token)).unwrap().as_slice(),
                        bincode_config.serialize(tf).unwrap().as_slice(),
                    )
                    .unwrap();
            }
            token_ids.dedup();
            count_tx
                .send((file_id, token_ids))
                .expect("Count send failed");
        }
    });

    // index
    let index_tree = db.open_tree("index").unwrap();
    let handle = std::thread::spawn(move || {
        let mut process_count = 0;
        let mut index: HashMap<TokenID, HashSet<FileID>> = HashMap::new();
        let mut all_token_ids = HashSet::new();
        for (file_id, token_ids) in count_rx {
            for token_id in &token_ids {
                match index.get_mut(&token_id) {
                    Some(s) => {
                        s.insert(file_id);
                    }
                    None => {
                        let mut s = HashSet::new();
                        s.insert(file_id);
                        index.insert(*token_id, s);
                    }
                }
            }
            all_token_ids.extend(token_ids);
            process_count += 1;
            eprint!("\rProcess file: {}", process_count);
            db.insert(
                bincode_config.serialize("file_count").unwrap().as_slice(),
                bincode_config.serialize(&process_count).unwrap().as_slice(),
            )
            .unwrap();
        }

        db.insert(
            bincode_config.serialize("tokens").unwrap().as_slice(),
            bincode_config.serialize(&all_token_ids).unwrap().as_slice(),
        ).unwrap();

        for (token_id, set) in &index {
            tf_df_tree1
                .insert(
                    bincode_config.serialize(token_id).unwrap().as_slice(),
                    bincode_config.serialize(&set.len()).unwrap().as_slice(),
                )
                .unwrap();
            index_tree
                .insert(
                    bincode_config.serialize(token_id).unwrap().as_slice(),
                    bincode_config.serialize(set).unwrap().as_slice(),
                )
                .unwrap();
        }
    });

    handle.join().expect("Index error");

    println!();

    Ok(())
}
