use std::collections::HashSet;

use rust_stemmers::Stemmer;
use rust_tokenizers::tokenizer::{BertTokenizer, Tokenizer};
use rust_tokenizers::vocab::{BertVocab, Vocab};
use stopwords::{Language, Spark, Stopwords};

use bincode;
use bincode::config::Options;
use sled;

use crossbeam;
use rayon::prelude::*;

use text_io::try_read;

mod index;
use index::{BERT_VOCAB_PATH, DB_PATH};

use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;

pub enum BiOp {
    And,
    Or,
}
pub enum UnOp {
    Not,
}
pub enum Query {
    KeyWord(String),
    UnOpQuery(UnOp, Box<Query>),
    BiOpQuery(Box<Query>, BiOp, Box<Query>),
}

fn main() {
    let vocab_path = BERT_VOCAB_PATH;
    let vocab = BertVocab::from_file(&vocab_path).expect("Failed to load vocab");
    let tokenizer = BertTokenizer::from_existing_vocab(vocab, true, false);
    let stemmer = Stemmer::create(rust_stemmers::Algorithm::English);
    let stops: HashSet<_> = Spark::stopwords(Language::English)
        .unwrap()
        .iter()
        .map(|&x| x.to_string())
        .collect();

    // init serialization
    let bincode_config = bincode::options().with_big_endian();

    // init db
    eprintln!("Loading index...");
    let db: sled::Db = sled::open(DB_PATH).unwrap();
    let files_tree = db.open_tree("files").unwrap();
    let index_tree = db.open_tree("index").unwrap();
    let tf_df_tree = db.open_tree("tf_df").unwrap();

    eprintln!("OK");
    loop {
        eprint!("Q> ");
        let query: String = match try_read!("{}\n") {
            Ok(s) => s,
            _ => break,
        };
        let tokens = tokenizer.tokenize(query);
        let tokens: Vec<String> = tokens
            .into_iter()
            .filter(|x| !stops.contains(x) && x.len() > 1)
            .collect();
        let token_ids = tokenizer.convert_tokens_to_ids(&tokens);

        // Get files of term in query
        let mut sets: Vec<HashSet<u64>> = token_ids
            .iter()
            .map(|token_id| -> HashSet<u64> {
                bincode_config
                    .deserialize(
                        index_tree
                            .get(bincode_config.serialize(token_id).unwrap().as_slice())
                            .unwrap()
                            .unwrap_or(sled::IVec::from(""))
                            .to_vec()
                            .as_slice(),
                    )
                    .unwrap_or(HashSet::new())
            })
            .collect();

        let mut union: HashSet<u64> = match sets.pop() {
            Some(f) => f,
            _ => HashSet::new(),
        };

        for s in &sets {
            union = union.union(s).map(|x| *x).collect();
        }

        let file_count: i32 = bincode_config
            .deserialize(
                db.get(bincode_config.serialize("file_count").unwrap())
                    .unwrap()
                    .unwrap()
                    .to_vec()
                    .as_slice(),
            )
            .unwrap();

        let mut cos_pq = PriorityQueue::new();

        let (tx, rx) = crossbeam::channel::bounded(1024);
        let tf_df_tree1 = tf_df_tree.clone();
        std::thread::spawn(move || {
            union.into_par_iter().for_each(|each_file| {
                let (ab, a, b) = token_ids
                    .par_iter()
                    .map(|each_token| {
                        let tf_raw: u64 = bincode_config
                            .deserialize(
                                tf_df_tree1
                                    .get(
                                        bincode_config
                                            .serialize(&(each_file, *each_token))
                                            .unwrap()
                                            .as_slice(),
                                    )
                                    .unwrap()
                                    .unwrap_or(sled::IVec::from(""))
                                    .to_vec()
                                    .as_slice(),
                            )
                            .unwrap_or(0);
                        let mut tf = 0.0;
                        if tf_raw != 0 {
                            tf = 1.0 + (tf_raw as f32).log10();
                        }
                        let df: usize = bincode_config
                            .deserialize(
                                tf_df_tree1
                                    .get(bincode_config.serialize(each_token).unwrap().as_slice())
                                    .unwrap()
                                    .unwrap()
                                    .to_vec()
                                    .as_slice(),
                            )
                            .unwrap();
                        let tfidf = tf * (file_count as f32 / df as f32).log10();
                        (each_token, tfidf)
                    })
                    .fold(
                        || (0f32, 0f32, 0f32),
                        |result, token_id_and_tfidf| {
                            let (ab, a, b) = result;
                            let (token_id, tfidf) = token_id_and_tfidf;
                            if token_ids.contains(token_id) {
                                (ab + tfidf, a + 1f32, b + tfidf * tfidf)
                            } else {
                                (ab, a, b + tfidf * tfidf)
                            }
                        },
                    )
                    .reduce(
                        || (0f32, 0f32, 0f32),
                        |a, b| {
                            let (ab1, a1, b1) = a;
                            let (ab2, a2, b2) = b;
                            (ab1 + ab2, a1 + a2, b1 + b2)
                        },
                    );
                let cos = ab / (a.sqrt() * b.sqrt());
                tx.send((each_file, cos)).unwrap();
            })
        });

        for (each_file, cos) in rx {
            cos_pq.push(each_file, OrderedFloat::from(cos));
        }

        let result_count = cos_pq.len();
        for (file_id, cos) in cos_pq.into_sorted_iter().take(10) {
            let path = files_tree
                .get(bincode_config.serialize(&file_id).unwrap().as_slice())
                .unwrap()
                .unwrap();
            let file_path = String::from_utf8_lossy(path.as_ref());
            println!("{}, cos: {}", file_path, cos);
        }

        eprintln!("Found {} results", result_count);
        eprintln!("Tokens: [{}]", tokens.join(", "));
    }
}
