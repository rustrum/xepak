use std::{borrow::Cow, ops::RangeInclusive, str::CharIndices};

pub type KeyArgPositionRef<'a> = (&'a str, RangeInclusive<usize>);

pub struct ParametrizedQuery {
    query: String,
    args: Vec<String>,
    positions: Vec<RangeInclusive<usize>>,
}

impl ParametrizedQuery {
    pub fn new(query: String) -> Self {
        let mut args = Vec::new();
        let mut positions = Vec::new();
        for (arg, pos) in SqlLexer::new(&query) {
            args.push(arg.to_string());
            positions.push(pos);
        }
        Self {
            query,
            args,
            positions,
        }
    }

    pub fn has_args(&self) -> bool {
        !self.args.is_empty()
    }

    pub fn get_args(&self) -> &[String] {
        &self.args
    }

    /// Return original query
    pub fn get_query(&self) -> &str {
        &self.query
    }

    /// Convert key args query to pos args query.
    ///
    /// - `query` query with key arguments
    /// - `pos_arg` positional argument placeholder ("?" for SQL)
    ///
    /// Returns original query if no pos args available or query with pos args.
    pub fn build_query(&self, pos_arg: &str) -> String {
        build_pos_query(&self.query, &self.positions, pos_arg)
    }
}

/// Same as [`ParametrizedQuery`], but with a reference to the SQL string.
pub struct ParametrizedQueryRef<'a> {
    query: &'a str,
    args: Vec<&'a str>,
    positions: Vec<RangeInclusive<usize>>,
}

impl<'a> ParametrizedQueryRef<'a> {
    pub fn new(query: &'a str) -> Self {
        let mut args = Vec::new();
        let mut positions = Vec::new();
        for (arg, pos) in SqlLexer::new(&query) {
            args.push(arg);
            positions.push(pos);
        }

        Self {
            query,
            args,
            positions,
        }
    }

    pub fn has_args(&self) -> bool {
        !self.args.is_empty()
    }

    pub fn get_args(&self) -> &[&'a str] {
        &self.args
    }

    /// Return original query
    pub fn get_query(&self) -> &'a str {
        self.query
    }

    /// Convert key args query to pos args query.
    ///
    /// - `query` query with key arguments
    /// - `pos_arg` positional argument placeholder ("?" for SQL)
    ///
    /// Returns original query if no pos args available or query with pos args.
    pub fn build_query(&self, pos_arg: &str) -> Cow<'a, str> {
        if self.has_args() {
            Cow::Owned(build_pos_query(self.query, &self.positions, pos_arg))
        } else {
            Cow::Borrowed(self.query)
        }
    }
}

fn build_pos_query(query: &str, ranges: &[RangeInclusive<usize>], pos_arg: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let mut last_index = 0;

    for rng in ranges {
        let start = *rng.start();
        let end = *rng.end() + 1;
        // Append the text before the range
        if start > last_index {
            result.push_str(&query[last_index..start]);
        }

        // Append the placeholder
        result.push_str(pos_arg);

        // Move the cursor forward
        last_index = last_index.max(end);
    }

    // Append any remaining text after the last range
    if last_index < query.len() {
        result.push_str(&query[last_index..]);
    }

    result
}

/// Replace key args with pos args in input.
/// NOTE `key_args` input expected to be sorted according to appearance in the `query`.
pub fn query_to_pos_args(query: &str, pos_arg: &str, key_args: &[KeyArgPositionRef]) -> String {
    let mut result = String::new();
    let mut offset_from = 0;
    for (_, range) in key_args {
        let slice = offset_from..*range.start();
        let Some(subslice) = query.get(slice) else {
            break;
        };
        result.push_str(subslice);
        result.push_str(pos_arg);
        offset_from = *range.end() + 1;
    }
    result
}

pub struct SqlLexer<'a> {
    sql: &'a str,
    sql_index: CharIndices<'a>,
    offset: usize,
}

impl<'a> SqlLexer<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql,
            sql_index: sql.char_indices(),
            offset: 0,
        }
    }

    pub fn into_key_args(self) -> Vec<KeyArgPositionRef<'a>> {
        let mut result = Vec::new();
        for karg in self {
            result.push(karg);
        }
        result
    }

    fn next_char(&mut self) -> Option<char> {
        self.offset = self.sql_index.offset();
        if let Some((_char_offset, c)) = self.sql_index.next() {
            Some(c)
        } else {
            None
        }
    }

    pub fn next_key_arg(&mut self) -> Option<KeyArgPositionRef<'a>> {
        let mut state = LexerState::Sql;
        while let Some(cur_char) = self.next_char() {
            // DEBUG \(^_^)/
            // println!("{} -> {:?}", self.offset, state);
            match cur_char {
                '\\' => match state {
                    LexerState::StringSingle { escape } => {
                        state = LexerState::StringSingle { escape: !escape }
                    }
                    LexerState::StringDouble { escape } => {
                        state = LexerState::StringDouble { escape: !escape }
                    }
                    LexerState::Sql
                    | LexerState::StringSingleClosing
                    | LexerState::StringDoubleClosing => state = LexerState::Sql,
                    _ => {}
                },
                '\'' => match state {
                    LexerState::Sql => state = LexerState::StringSingle { escape: false },
                    LexerState::StringSingle { escape } if escape => {
                        state = LexerState::StringSingle { escape: false }
                    }
                    LexerState::StringSingle { escape } if !escape => {
                        state = LexerState::StringSingleClosing
                    }
                    LexerState::StringSingleClosing => {
                        state = LexerState::StringSingle { escape: false }
                    }
                    LexerState::StringDoubleClosing => state = LexerState::Sql,
                    _ => {}
                },
                '"' => match state {
                    LexerState::Sql => state = LexerState::StringDouble { escape: false },
                    LexerState::StringDouble { escape } if escape => {
                        state = LexerState::StringDouble { escape: false }
                    }
                    LexerState::StringDouble { escape } if !escape => {
                        state = LexerState::StringDoubleClosing
                    }
                    LexerState::StringDoubleClosing => {
                        state = LexerState::StringDouble { escape: false }
                    }
                    LexerState::StringSingleClosing => state = LexerState::Sql,
                    _ => {}
                },
                '{' => match state {
                    LexerState::Sql
                    | LexerState::StringSingleClosing
                    | LexerState::StringDoubleClosing => {
                        state = LexerState::CurlOpen(1, self.offset);
                    }
                    LexerState::CurlOpen(c, offset) if c == 1 => {
                        state = LexerState::CurlOpen(2, offset);
                    }
                    LexerState::CurlOpen(c, _) if c > 2 => state = LexerState::Sql,
                    _ => {}
                },
                '}' => match state {
                    LexerState::StringSingleClosing | LexerState::StringDoubleClosing => {
                        state = LexerState::Sql;
                    }
                    LexerState::CurlOpen(c, _) if c != 2 => {
                        state = LexerState::Sql;
                    }
                    LexerState::CurlOpen(c, offset) if c == 2 => {
                        state = LexerState::CurlClose(1, offset);
                    }
                    LexerState::CurlClose(c, offset) if c == 1 => {
                        let from = offset;
                        let to = self.offset;
                        let key = &self.sql[(from + 2)..=(to - 2)];
                        return Some((key, from..=to));
                    }
                    LexerState::CurlClose(c, _) if c != 1 => {
                        // WTF
                        state = LexerState::Sql;
                    }
                    _ => {}
                },
                ch => match state {
                    LexerState::CurlOpen(c, _)
                        if c == 2 && ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' => {}
                    LexerState::CurlOpen(_, _)
                    | LexerState::CurlClose(_, _)
                    | LexerState::StringSingleClosing
                    | LexerState::StringDoubleClosing => {
                        state = LexerState::Sql;
                    }
                    _ => {}
                },
            }
        }
        None
    }
}

impl<'a> Iterator for SqlLexer<'a> {
    type Item = KeyArgPositionRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_key_arg()
    }
}

#[derive(Debug)]
enum LexerState {
    Sql,
    CurlOpen(usize, usize),
    CurlClose(usize, usize),
    StringSingle { escape: bool },
    StringSingleClosing,
    StringDouble { escape: bool },
    StringDoubleClosing,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn lexer_next_none() {
        let mut lexer = SqlLexer::new("SELECT * FROM table WHERE id = '1'");
        let karg = lexer.next_key_arg();
        assert!(karg.is_none(), "{karg:?}");

        let mut lexer =
            SqlLexer::new("SELECT * FROM table WHERE test = '{{notkey1}}' OR \"{{notkey2}}\"");
        let karg = lexer.next_key_arg();
        assert!(karg.is_none(), "{karg:?}");

        let mut lexer = SqlLexer::new("SELECT '\\'{{notkey1}}\\'' OR \"\\\"{{notkey2}}\\\"\"");
        let karg = lexer.next_key_arg();
        assert!(karg.is_none(), "{karg:?}");

        let mut lexer =
            SqlLexer::new("SELECT '\\'''{{notkey1}}''\\'' OR \"\\\"\"\"{{notkey2}}\"\"\\\"\"");
        let karg = lexer.next_key_arg();
        assert!(karg.is_none(), "{karg:?}");

        let mut lexer = SqlLexer::new(
            "{{not a key}} OR {{not/akey}} {{ not_key1 }} {{ not_key2}} {{not_key3 }} { {not_key4} } { {not_key5}} {{not_key6} }",
        );
        let karg = lexer.next_key_arg();
        assert!(karg.is_none(), "{karg:?}");

        let mut lexer = SqlLexer::new("{{notkey1},{notkey2}} {{notkey3} {{notkey4} {notkey5}");
        let karg = lexer.next_key_arg();
        assert!(karg.is_none(), "{karg:?}");
    }

    #[test]
    fn lexer_next_ok() {
        let mut lexer = SqlLexer::new("SELECT 'it''s a text' {{key1}}");
        let (key, range) = lexer.next_key_arg().unwrap();
        assert_eq!(key, "key1");
        assert_eq!(range, 22..=29);

        let mut lexer =
            SqlLexer::new("SELECT {{key1}} WHERE x={{key_2}} {{notkey1},{notkey2}} y={{key-3}}");

        let (key, range) = lexer.next_key_arg().unwrap();
        assert_eq!(key, "key1");
        assert_eq!(range, 7..=14);

        let (key, range) = lexer.next_key_arg().unwrap();
        assert_eq!(key, "key_2");
        assert_eq!(range, 24..=32);

        let (key, range) = lexer.next_key_arg().unwrap();
        assert_eq!(key, "key-3");
        assert_eq!(range, 58..=66);

        assert!(lexer.next_key_arg().is_none());
        
        let mut lexer =
            SqlLexer::new("SELECT * FROM users LIMIT {{--LIMIT--}} OFFSET {{__OFFSET__}}");

        let (key, range) = lexer.next_key_arg().unwrap();
        assert_eq!(key, "--LIMIT--");
        assert_eq!(range, 26..=38);

        let (key, range) = lexer.next_key_arg().unwrap();
        assert_eq!(key, "__OFFSET__");
        assert_eq!(range, 47..=60);

        assert!(lexer.next_key_arg().is_none());
    }

    #[test]
    fn parametrized_query() {
        let query = "SELECT {{key1}} WHERE x={{key2}} {{notkey1},{notkey2}} y={{key3}}";
        let query_simple = "SELECT * FROM table WHERE id = '1'";

        let qa = ParametrizedQueryRef::new(query);
        let pos_query = qa.build_query("?");

        assert_eq!(
            pos_query.to_string(),
            "SELECT ? WHERE x=? {{notkey1},{notkey2}} y=?"
        );

        let qa = ParametrizedQueryRef::new(query_simple);
        let pos_query = qa.build_query("?");

        assert_eq!(pos_query.to_string(), query_simple);
    }
}
