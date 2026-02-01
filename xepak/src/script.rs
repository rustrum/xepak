use rhai::{AST, CustomType, Engine, ParseError};

use crate::{XepakError, server::RequestArgs};

#[derive(Debug, Clone)]
pub struct RhaiRequestContext {
    args: RequestArgs,
}

impl From<RequestArgs> for RhaiRequestContext {
    fn from(args: RequestArgs) -> Self {
        Self { args }
    }
}

impl RhaiRequestContext {
    pub fn has_arg(&mut self, arg_name: &str) -> bool {
        self.args.has_any_arg(arg_name)
    }
}

impl CustomType for RhaiRequestContext {
    fn build(mut builder: rhai::TypeBuilder<Self>) {
        builder.with_name("RhaiRequestContext");
        builder.with_fn("has_arg", Self::has_arg);
    }
}

#[derive(Debug, Clone)]
pub struct RhaiQueryBuilder {
    query: Vec<String>,
}

impl RhaiQueryBuilder {
    pub fn new() -> Self {
        Self {
            query: Default::default(),
        }
    }

    pub fn new_str(q: String) -> Self {
        Self { query: vec![q] }
    }

    pub fn add(&mut self, part: String) {
        self.query.push(part);
    }

    pub fn add_joined_parts(
        &mut self,
        prefix: String,
        parts: rhai::Array,
        separator: String,
        suffix: String,
    ) {
        if parts.is_empty() {
            return;
        }

        if !prefix.is_empty() {
            self.query.push(prefix);
        }

        let parts: Vec<String> = parts.iter().map(|v| v.to_string()).collect();

        self.query.push(parts.join(&format!(" {separator} ")));

        if !suffix.is_empty() {
            self.query.push(suffix);
        }
    }

    pub fn build(&mut self) -> String {
        self.query.join(" ")
    }
}

impl CustomType for RhaiQueryBuilder {
    fn build(mut builder: rhai::TypeBuilder<Self>) {
        builder.with_name("RhaiQueryBuilder");
        builder.with_fn("query_builder", RhaiQueryBuilder::new);
        builder.with_fn("query_builder", RhaiQueryBuilder::new_str);
        builder.with_fn("add", Self::add);
        builder.with_fn("add_joined_parts", Self::add_joined_parts);
        builder.with_fn("build", Self::build);
    }
}

pub fn init_rhai_script(script: &str) -> Result<(Engine, AST), XepakError> {
    let rhai = build_rhai_engine();
    let ast = build_rhai_ast(&rhai, script)?;
    Ok((rhai, ast))
}

pub fn build_rhai_ast(rhai: &Engine, script: &str) -> Result<AST, ParseError> {
    let ast = rhai.compile(script)?;
    Ok(ast)
}

pub fn build_rhai_engine() -> Engine {
    let mut rhai = Engine::new();
    rhai.build_type::<RhaiRequestContext>();
    rhai.build_type::<RhaiQueryBuilder>();
    rhai
}
