use std::collections::HashSet;

// use rust_stemmers::Stemmer;
use rust_tokenizers::tokenizer::{BertTokenizer, Tokenizer};
use rust_tokenizers::vocab::{BertVocab, Vocab};
use stopwords::{Language, Spark, Stopwords};

use bincode;
use bincode::config::Options;
use sled;

#[macro_use]
extern crate lalrpop_util;

use search_engine::aaa::Query;
lalrpop_mod!(pub query);
use text_io::try_read;

mod index;
use index::{BERT_VOCAB_PATH, DB_PATH};

struct QueryExecutor<'a> {
    tokenizer: &'a BertTokenizer,
    stops: &'a HashSet<String>,
    index_tree: &'a sled::Tree,
    bincode_config:
        &'a bincode::config::WithOtherEndian<bincode::DefaultOptions, bincode::config::BigEndian>,
}

impl QueryExecutor<'_> {
    fn execute_query(&self, q: &Query) -> HashSet<u64> {
        match q {
            Query::KeyWord(s) => {
                let tokens = self.tokenizer.tokenize(s);
                let tokens: Vec<String> = tokens
                    .into_iter()
                    .filter(|x| !self.stops.contains(x) && x.len() > 1)
                    .collect();
                let token_ids = self.tokenizer.convert_tokens_to_ids(&tokens);

                let mut sets: Vec<HashSet<u64>> = token_ids
                    .iter()
                    .map(|token_id| -> HashSet<u64> {
                        self.bincode_config
                            .deserialize(
                                self.index_tree
                                    .get(
                                        self.bincode_config.serialize(token_id).unwrap().as_slice(),
                                    )
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
                intersection
            }
            Query::Diff(p, q) => {
                let ps = self.execute_query(p.as_ref());
                let qs = self.execute_query(q.as_ref());
                ps.difference(&qs).map(|x| *x).collect()
            }
            Query::And(p, q) => {
                let ps = self.execute_query(p.as_ref());
                let qs = self.execute_query(q.as_ref());
                ps.intersection(&qs).map(|x| *x).collect()
            }
            Query::Or(p, q) => {
                let ps = self.execute_query(p.as_ref());
                let qs = self.execute_query(q.as_ref());
                ps.union(&qs).map(|x| *x).collect()
            }
        }
    }
}

fn main() {
    let vocab_path = BERT_VOCAB_PATH;
    let vocab = BertVocab::from_file(&vocab_path).expect("Failed to load vocab");
    let tokenizer = BertTokenizer::from_existing_vocab(vocab, true, false);
    // let stemmer = Stemmer::create(rust_stemmers::Algorithm::English);
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

    let query_parser = query::QueryParser::new();
    let query_executor = QueryExecutor {
        tokenizer: &tokenizer,
        stops: &stops,
        index_tree: &index_tree,
        bincode_config: &bincode_config,
    };
    loop {
        eprint!("Q> ");
        let query_string_result: Result<String, _> = try_read!("{}\n");
        let query_string: String;
        let query: Query = match query_string_result {
            Ok(s) => {
                query_string = s;
                query_parser.parse(query_string.as_str())
            }
            _ => break,
        }
        .unwrap();

        let result = query_executor.execute_query(&query);

        for file_id in &result {
            let path = files_tree
                .get(bincode_config.serialize(file_id).unwrap().as_slice())
                .unwrap()
                .unwrap();
            let file_path = String::from_utf8_lossy(path.as_ref());
            println!("{}", file_path)
        }

        eprintln!("Found {} results", result.len());
        eprintln!("Tokens: [{:?}]", query);
    }
}
