use std::{
    mem::offset_of,
    ops::RangeInclusive,
    str::{CharIndices, Chars},
};

pub type KeyArgPosition<'a> = (&'a str, RangeInclusive<usize>);

pub struct SqlKpStruct<T> {
    pub sql: T,
    pub args: T,
}

/// Convert key args query to pos args query.
///
/// - `query` query with key arguments
/// - `pos_arg` positional argument placeholder ("?" for SQL)
///
/// Returns pos arg query and list of positional arguments names in order they appear in the query.
pub fn query_key_to_pos_args(query: &str, pos_arg: &str) -> (String, Vec<String>) {
    unimplemented!()
}

/// Replace key args with pos args in input.
/// NOTE `key_args` input expected to be sorted according to appearance in the `query`.
pub fn query_to_pos_args(query: &str, pos_arg: &str, key_args: &[KeyArgPosition]) -> String {
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
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            sql_index: sql.char_indices(),
            offset: 0,
        }
    }

    pub fn into_key_args(self) -> Vec<KeyArgPosition<'a>> {
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

    pub fn next_key_arg(&mut self) -> Option<KeyArgPosition<'a>> {
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
    type Item = KeyArgPosition<'a>;

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
            SqlLexer::new("SELECT {{key1}} WHERE x={{key2}} {{notkey1},{notkey2}} y={{key3}}");

        let (key, range) = lexer.next_key_arg().unwrap();
        assert_eq!(key, "key1");
        assert_eq!(range, 7..=14);

        let (key, range) = lexer.next_key_arg().unwrap();
        assert_eq!(key, "key2");
        assert_eq!(range, 24..=31);

        let (key, range) = lexer.next_key_arg().unwrap();
        assert_eq!(key, "key3");
        assert_eq!(range, 57..=64);

        assert!(lexer.next_key_arg().is_none());
    }
}
