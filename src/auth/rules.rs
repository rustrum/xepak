use std::collections::HashSet;

use crate::XepakError;

/// Authorization rules parsed from rules expression
#[derive(Debug, Clone)]
pub enum AuthRules {
    Role {
        v: String,
    },

    Id {
        v: String,
    },

    /// Combine nested conditions via logical AND (all must succeed)
    And {
        nested: Vec<AuthRules>,
    },

    /// Combine nested conditions via logical OR (at least one must succeed)
    Or {
        nested: Vec<AuthRules>,
    },
}

impl PartialEq for AuthRules {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (AuthRules::Role { v: a_v }, AuthRules::Role { v: b_v }) => a_v == b_v,
            (AuthRules::Id { v: a_v }, AuthRules::Id { v: b_v }) => a_v == b_v,
            (AuthRules::And { nested: a_n }, AuthRules::And { nested: b_n }) => {
                a_n.len() == b_n.len() && a_n.iter().zip(b_n).all(|(a, b)| a == b)
            }
            (AuthRules::Or { nested: a_n }, AuthRules::Or { nested: b_n }) => {
                a_n.len() == b_n.len() && a_n.iter().zip(b_n).all(|(a, b)| a == b)
            }
            _ => false,
        }
    }
}

impl Eq for AuthRules {}

impl AuthRules {
    /// Just make all roles uppercase
    pub fn normalize(&self) -> Self {
        match self {
            AuthRules::Role { v } => Self::Role {
                v: v.clone().to_uppercase(),
            },
            AuthRules::Id { .. } => self.clone(),
            AuthRules::And { nested } => Self::And {
                nested: nested.iter().cloned().map(|v| v.normalize()).collect(),
            },
            AuthRules::Or { nested } => Self::And {
                nested: nested.iter().cloned().map(|v| v.normalize()).collect(),
            },
        }
    }

    pub fn is_allowed(&self, id: &str, roles: &HashSet<String>) -> bool {
        match self {
            AuthRules::Role { v } => roles.contains(v),
            AuthRules::Id { v } => id == v,
            AuthRules::And { nested } => {
                let mut check = true;
                for c in nested {
                    if !c.is_allowed(id, roles) {
                        check = false;
                        break;
                    }
                }
                check
            }
            AuthRules::Or { nested } => {
                let mut check = false;
                for c in nested {
                    if c.is_allowed(id, roles) {
                        check = true;
                        break;
                    }
                }
                check
            }
        }
    }
}
pub struct RulesParser<'a> {
    input: &'a str,
    scopes: Vec<Scope>,
    state: ParseState,
    token_buf: String,
    pos: usize,
}

impl<'a> RulesParser<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            scopes: vec![Scope::new()],
            state: ParseState::Idle,
            token_buf: String::new(),
            pos: 0,
        }
    }

    pub fn parse(&mut self) -> Result<AuthRules, XepakError> {
        for (idx, ch) in self.input.char_indices() {
            self.pos = idx;
            self.process_char(ch)?;
        }

        if self.state != ParseState::Idle {
            self.flush_token()?;
        }

        if self.scopes.len() != 1 {
            return Err(parse_err("missing closing brace", self.pos));
        }

        let root = self.scopes.pop().unwrap();
        if root.items.is_empty() {
            return Err(parse_err("empty auth string", self.pos));
        }

        Ok(root.combine())
    }

    fn process_char(&mut self, ch: char) -> Result<(), XepakError> {
        match self.state {
            ParseState::Idle => match ch {
                ' ' | '\t' | '\n' | '\r' => Ok(()),
                '(' => self.push_scope(self.pos),
                ')' => self.pop_scope(self.pos),
                '#' => {
                    self.state = ParseState::ReadingId;
                    self.token_buf.clear();
                    Ok(())
                }
                c if c.is_lowercase() => {
                    self.state = ParseState::ReadingRole;
                    self.token_buf.clear();
                    self.token_buf.push(c);
                    Ok(())
                }
                c if c.is_uppercase() => {
                    self.state = ParseState::ReadingKeyword;
                    self.token_buf.clear();
                    self.token_buf.push(c);
                    Ok(())
                }
                _ => Err(parse_err("unexpected character", self.pos)),
            },

            ParseState::ReadingRole => match ch {
                ' ' | '\t' | '\n' | '\r' | '(' | ')' => {
                    let word = self.flush_to_idle();
                    self.consume_word(&word, self.pos, true)?;
                    self.handle_brace(ch)
                }
                c if c.is_alphanumeric() || c == '_' => {
                    self.token_buf.push(c);
                    Ok(())
                }
                _ => Err(parse_err("unexpected character", self.pos)),
            },

            ParseState::ReadingId => match ch {
                ' ' | '\t' | '\n' | '\r' | '(' | ')' => {
                    let word = self.flush_to_idle();
                    self.consume_id(&word, self.pos)?;
                    self.handle_brace(ch)
                }
                c if c.is_alphanumeric() || c == '_' => {
                    self.token_buf.push(c);
                    Ok(())
                }
                _ => Err(parse_err("unexpected character", self.pos)),
            },

            ParseState::ReadingKeyword => match ch {
                ' ' | '\t' | '\n' | '\r' | '(' | ')' => {
                    let word = self.flush_to_idle();
                    self.consume_word(&word, self.pos, false)?;
                    self.handle_brace(ch)
                }
                c if c.is_alphanumeric() || c == '_' => {
                    self.token_buf.push(c);
                    Ok(())
                }
                _ => Err(parse_err("unexpected character", self.pos)),
            },
        }
    }

    fn flush_token(&mut self) -> Result<(), XepakError> {
        let word = std::mem::take(&mut self.token_buf);

        match self.state {
            ParseState::ReadingRole => self.consume_word(&word, self.pos, true),
            ParseState::ReadingId => self.consume_id(&word, self.pos),
            ParseState::ReadingKeyword => self.consume_word(&word, self.pos, false),
            _ => Ok(()),
        }
    }

    fn flush_to_idle(&mut self) -> String {
        self.state = ParseState::Idle;
        std::mem::take(&mut self.token_buf)
    }

    fn handle_brace(&mut self, ch: char) -> Result<(), XepakError> {
        match ch {
            '(' => self.push_scope(self.pos),
            ')' => self.pop_scope(self.pos),
            _ => Ok(()),
        }
    }

    fn consume_word(&mut self, word: &str, idx: usize, allow_role: bool) -> Result<(), XepakError> {
        if let Some(new_op) = classify_keyword(word, idx)? {
            let scope = self.scopes.last_mut().unwrap();
            scope.op = scope.op.on_operator(new_op, idx)?;
        } else if allow_role {
            let scope = self.scopes.last_mut().unwrap();
            if matches!(scope.op, OpKind::Empty) && !scope.items.is_empty() {
                return Err(parse_err("expected operator between operands", idx));
            }
            scope.items.push(AuthRules::Role {
                v: word.to_string(),
            });
            scope.op = scope.op.on_operand();
        }
        Ok(())
    }

    fn consume_id(&mut self, word: &str, idx: usize) -> Result<(), XepakError> {
        if word.is_empty() {
            return Err(parse_err("empty id after '#'", idx));
        }
        let scope = self.scopes.last_mut().unwrap();
        if matches!(scope.op, OpKind::Empty) && !scope.items.is_empty() {
            return Err(parse_err("expected operator between operands", idx));
        }
        scope.items.push(AuthRules::Id {
            v: word.to_string(),
        });
        scope.op = scope.op.on_operand();
        Ok(())
    }

    fn push_scope(&mut self, idx: usize) -> Result<(), XepakError> {
        let scope = self.scopes.last().unwrap();
        if !scope.op.expecting_operand() && !scope.items.is_empty() {
            return Err(parse_err("expected operator before '('", idx));
        }
        self.scopes.push(Scope::new());
        Ok(())
    }

    fn pop_scope(&mut self, idx: usize) -> Result<(), XepakError> {
        if self.scopes.len() <= 1 {
            return Err(parse_err("unexpected ')'", idx));
        }
        let inner = self.scopes.pop().unwrap();
        if inner.items.is_empty() {
            return Err(parse_err("empty group", idx));
        }
        let combined = inner.combine();
        let scope = self.scopes.last_mut().unwrap();
        scope.items.push(combined);
        scope.op = scope.op.on_operand();
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Empty,
    And,
    AndExpecting,
    Or,
    OrExpecting,
}

impl OpKind {
    fn expecting_operand(&self) -> bool {
        matches!(self, OpKind::AndExpecting | OpKind::OrExpecting)
    }

    fn on_operator(&self, new: OpKind, position: usize) -> Result<OpKind, XepakError> {
        match (self, new) {
            (OpKind::Empty, OpKind::And) => Ok(OpKind::AndExpecting),
            (OpKind::Empty, OpKind::Or) => Ok(OpKind::OrExpecting),
            (OpKind::And | OpKind::AndExpecting, OpKind::And) => Ok(OpKind::AndExpecting),
            (OpKind::Or | OpKind::OrExpecting, OpKind::Or) => Ok(OpKind::OrExpecting),
            _ => Err(parse_err("mixed AND/OR operators inside braces", position)),
        }
    }

    fn on_operand(&self) -> OpKind {
        match self {
            OpKind::AndExpecting => OpKind::And,
            OpKind::OrExpecting => OpKind::Or,
            other => *other,
        }
    }
}

struct Scope {
    items: Vec<AuthRules>,
    op: OpKind,
}

impl Scope {
    fn new() -> Self {
        Self {
            items: Vec::new(),
            op: OpKind::Empty,
        }
    }

    fn combine(self) -> AuthRules {
        match self.items.len() {
            0 => unreachable!(),
            1 => self.items.into_iter().next().unwrap(),
            _ => match self.op {
                OpKind::And | OpKind::AndExpecting => AuthRules::And { nested: self.items },
                OpKind::Or | OpKind::OrExpecting => AuthRules::Or { nested: self.items },
                OpKind::Empty => unreachable!(),
            },
        }
    }
}

fn parse_err(message: &str, pos: usize) -> XepakError {
    XepakError::Cfg(format!(
        "Auth string parse error at position {pos}: {message}"
    ))
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ParseState {
    Idle,
    ReadingRole,
    ReadingId,
    ReadingKeyword,
}

fn classify_keyword(word: &str, pos: usize) -> Result<Option<OpKind>, XepakError> {
    match word {
        "AND" => Ok(Some(OpKind::And)),
        "OR" => Ok(Some(OpKind::Or)),
        w if w.to_uppercase() == "AND" => Err(parse_err("'AND' keyword must be uppercase", pos)),
        w if w.to_uppercase() == "OR" => Err(parse_err("'OR' keyword must be uppercase", pos)),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_simple() {
        let mut parser = RulesParser::new("user");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Role {
                v: "user".to_string()
            }
        );
    }

    #[test]
    fn test_id_simple() {
        let mut parser = RulesParser::new("#boss");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Id {
                v: "boss".to_string()
            }
        );
    }

    #[test]
    fn test_and_two_roles() {
        let mut parser = RulesParser::new("manager AND billing");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::And {
                nested: vec![
                    AuthRules::Role {
                        v: "manager".to_string()
                    },
                    AuthRules::Role {
                        v: "billing".to_string()
                    }
                ]
            }
        );
    }

    #[test]
    fn test_and_three_roles() {
        let mut parser = RulesParser::new("manager AND billing AND accounting");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::And {
                nested: vec![
                    AuthRules::Role {
                        v: "manager".to_string()
                    },
                    AuthRules::Role {
                        v: "billing".to_string()
                    },
                    AuthRules::Role {
                        v: "accounting".to_string()
                    }
                ]
            }
        );
    }

    #[test]
    fn test_and_role_with_id() {
        let mut parser = RulesParser::new("admin AND #superID");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::And {
                nested: vec![
                    AuthRules::Role {
                        v: "admin".to_string()
                    },
                    AuthRules::Id {
                        v: "superID".to_string()
                    }
                ]
            }
        );
    }

    #[test]
    fn test_or_two_roles() {
        let mut parser = RulesParser::new("manager OR billing");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Or {
                nested: vec![
                    AuthRules::Role {
                        v: "manager".to_string()
                    },
                    AuthRules::Role {
                        v: "billing".to_string()
                    }
                ]
            }
        );
    }

    #[test]
    fn test_or_three_roles() {
        let mut parser = RulesParser::new("billing OR accounting OR #super_manager_id");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Or {
                nested: vec![
                    AuthRules::Role {
                        v: "billing".to_string()
                    },
                    AuthRules::Role {
                        v: "accounting".to_string()
                    },
                    AuthRules::Id {
                        v: "super_manager_id".to_string()
                    }
                ]
            }
        );
    }

    #[test]
    fn test_braced_and_or() {
        let mut parser = RulesParser::new("(manager AND billing) OR #boss");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Or {
                nested: vec![
                    AuthRules::And {
                        nested: vec![
                            AuthRules::Role {
                                v: "manager".to_string()
                            },
                            AuthRules::Role {
                                v: "billing".to_string()
                            }
                        ]
                    },
                    AuthRules::Id {
                        v: "boss".to_string()
                    }
                ]
            }
        );
    }

    #[test]
    fn test_braced_and_three() {
        let mut parser =
            RulesParser::new("(manager AND billing AND accounting) OR (admin AND super)");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Or {
                nested: vec![
                    AuthRules::And {
                        nested: vec![
                            AuthRules::Role {
                                v: "manager".to_string()
                            },
                            AuthRules::Role {
                                v: "billing".to_string()
                            },
                            AuthRules::Role {
                                v: "accounting".to_string()
                            }
                        ]
                    },
                    AuthRules::And {
                        nested: vec![
                            AuthRules::Role {
                                v: "admin".to_string()
                            },
                            AuthRules::Role {
                                v: "super".to_string()
                            }
                        ]
                    }
                ]
            }
        );
    }

    #[test]
    fn test_nested_braces() {
        let mut parser = RulesParser::new("admin OR (manager AND (billing OR #superID))");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Or {
                nested: vec![
                    AuthRules::Role {
                        v: "admin".to_string()
                    },
                    AuthRules::And {
                        nested: vec![
                            AuthRules::Role {
                                v: "manager".to_string()
                            },
                            AuthRules::Or {
                                nested: vec![
                                    AuthRules::Role {
                                        v: "billing".to_string()
                                    },
                                    AuthRules::Id {
                                        v: "superID".to_string()
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        );
    }

    #[test]
    fn test_complex_nested() {
        let mut parser =
            RulesParser::new("manager AND (billing OR accounting OR #super_manager_id)");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::And {
                nested: vec![
                    AuthRules::Role {
                        v: "manager".to_string()
                    },
                    AuthRules::Or {
                        nested: vec![
                            AuthRules::Role {
                                v: "billing".to_string()
                            },
                            AuthRules::Role {
                                v: "accounting".to_string()
                            },
                            AuthRules::Id {
                                v: "super_manager_id".to_string()
                            }
                        ]
                    }
                ]
            }
        );
    }

    #[test]
    fn test_single_in_braces_unwrapped() {
        let mut parser = RulesParser::new("(user)");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Role {
                v: "user".to_string()
            }
        );
    }

    #[test]
    fn test_id_with_underscore() {
        let mut parser = RulesParser::new("#super_manager_id");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Id {
                v: "super_manager_id".to_string()
            }
        );
    }

    #[test]
    fn test_role_with_underscore() {
        let mut parser = RulesParser::new("super_manager AND billing");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::And {
                nested: vec![
                    AuthRules::Role {
                        v: "super_manager".to_string()
                    },
                    AuthRules::Role {
                        v: "billing".to_string()
                    }
                ]
            }
        );
    }

    #[test]
    fn test_whitespace_around_tokens() {
        let mut parser = RulesParser::new("  manager   AND   billing  ");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::And {
                nested: vec![
                    AuthRules::Role {
                        v: "manager".to_string()
                    },
                    AuthRules::Role {
                        v: "billing".to_string()
                    }
                ]
            }
        );
    }

    #[test]
    fn test_multiple_braces_nested() {
        let mut parser = RulesParser::new("(admin OR (manager AND (billing OR #superID)))");
        let conf = parser.parse().expect("should parse");
        assert_eq!(
            conf,
            AuthRules::Or {
                nested: vec![
                    AuthRules::Role {
                        v: "admin".to_string()
                    },
                    AuthRules::And {
                        nested: vec![
                            AuthRules::Role {
                                v: "manager".to_string()
                            },
                            AuthRules::Or {
                                nested: vec![
                                    AuthRules::Role {
                                        v: "billing".to_string()
                                    },
                                    AuthRules::Id {
                                        v: "superID".to_string()
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        );
    }

    #[test]
    fn test_parse_error_on_empty() {
        let mut parser = RulesParser::new("");
        let result = parser.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_not_uppercase_keywords() {
        let mut parser = RulesParser::new("manager and billing");
        let result = parser.parse();
        assert!(result.is_err());

        let mut parser = RulesParser::new("manager aNd billing");
        let result = parser.parse();
        assert!(result.is_err());

        let mut parser = RulesParser::new("manager or billing");
        let result = parser.parse();
        assert!(result.is_err());

        let mut parser = RulesParser::new("manager oR billing");
        let result = parser.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_on_mixed_operators_in_braces() {
        let mut parser = RulesParser::new("(manager AND billing OR boss)");
        let result = parser.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_on_missing_closing_brace() {
        let mut parser = RulesParser::new("(manager AND billing OR boss");
        let result = parser.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_on_empty_id_after_hash() {
        let mut parser = RulesParser::new("#");
        let result = parser.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_on_unexpected_character() {
        let mut parser = RulesParser::new("manager@billing");
        let result = parser.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_on_operands_without_operator() {
        let result = RulesParser::new("user admin").parse();
        assert!(result.is_err());

        let result = RulesParser::new("#id1 #id2").parse();
        assert!(result.is_err());

        let result = RulesParser::new("(user admin)").parse();
        assert!(result.is_err());
    }
}
