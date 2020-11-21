use std::collections::hash_set::Intersection;
use std::collections::HashSet;

use rust_stemmers::Stemmer;
use rust_tokenizers::tokenizer::{BertTokenizer, Tokenizer};
use rust_tokenizers::vocab::{BertVocab, Vocab};
use stopwords::{Language, Spark, Stopwords};

use bincode;
use bincode::config::Options;
use sled;

use text_io::try_read;

mod index;
use index::{BERT_VOCAB_PATH, DB_PATH};

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

        let mut intersection: HashSet<u64> = match sets.pop() {
            Some(f) => f,
            _ => HashSet::new(),
        };

        for s in &sets {
            intersection = intersection.intersection(s).map(|x| *x).collect();
        }

        for file_id in &intersection {
            let path = files_tree
                .get(bincode_config.serialize(file_id).unwrap().as_slice())
                .unwrap()
                .unwrap();
            let file_path = String::from_utf8_lossy(path.as_ref());
            println!("{}", file_path)
        }

        eprintln!("Found {} results", intersection.len());
        eprintln!("Tokens: [{}]", tokens.join(", "));
    }
}
