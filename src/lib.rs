pub mod aaa {
    #[derive(Debug)]
    pub enum Query {
        KeyWord(String),
        Diff(Box<Query>, Box<Query>),
        And(Box<Query>, Box<Query>),
        Or(Box<Query>, Box<Query>),
    }
}