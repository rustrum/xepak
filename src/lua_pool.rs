//! Attemp to implement per thread !Sync pool for Lua.
//! Do not want to add Sync to Lua because of performance considerations.
//!
//! This is an experimental solution for probably non existend issue.
//! Disabled for now
use mlua::Lua;
use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{XepakError, script_lua::build_lua_engine, server::XepakAppData};

static LUA_POOL_INIT_SIZE: AtomicU16 = AtomicU16::new(3);

/// Update initial lua pool size, returns normalized value that was applied.
/// Must be executed before application starts or would not work.
pub fn init_lua_pool_size(new_size: u16) -> u16 {
    let size = if new_size < 3 { 3 } else { new_size };
    LUA_POOL_INIT_SIZE.swap(size, Ordering::Relaxed);
    size
}

thread_local! {
    static LUA_POOL: LuaPool = {
        let size = LUA_POOL_INIT_SIZE.load(Ordering::Relaxed) as usize;
        LuaPool {
            pool: RefCell::new(Vec::with_capacity(size)),
            pool_max: size,
            permits: Arc::new(Semaphore::new(size)),
        }
    };
}

struct LuaPool {
    pool: RefCell<Vec<Lua>>,
    pool_max: usize,
    permits: Arc<Semaphore>,
}

impl LuaPool {
    fn load(&self, app_state: &XepakAppData) -> Result<Lua, XepakError> {
        let lua = match self.pool.borrow_mut().pop() {
            Some(l) => l,
            None => build_lua_engine(app_state)?,
        };

        Ok(lua)
    }

    fn push_back(&self, lua: Lua) {
        let mut pool = self.pool.borrow_mut();
        if pool.len() < self.pool_max {
            pool.push(lua);
        }
    }
}

pub struct LuaGuard {
    lua: Option<Lua>,
    _permit: OwnedSemaphorePermit,
}

impl LuaGuard {
    fn new(lua: Lua, _permit: OwnedSemaphorePermit) -> Self {
        Self {
            lua: Some(lua),
            _permit,
        }
    }
}

impl std::ops::Deref for LuaGuard {
    type Target = Lua;

    fn deref(&self) -> &Self::Target {
        self.lua.as_ref().expect("LuaGuard already dropped")
    }
}

impl std::ops::DerefMut for LuaGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.lua.as_mut().expect("LuaGuard already dropped")
    }
}

impl Drop for LuaGuard {
    fn drop(&mut self) {
        let lua = self.lua.take().unwrap();
        LUA_POOL.with(|p| p.push_back(lua));
        // _permit is dropped right after, releasing the semaphore and waking waiters.
    }
}

/// Retreive Lua engine for a thread local pool.
///
pub async fn pool_lua_engine<'a>(app_state: &XepakAppData) -> Result<LuaGuard, XepakError> {
    let permits = LUA_POOL.with(|p| p.permits.clone());
    let permit = permits
        .acquire_owned()
        .await
        .map_err(|e| XepakError::Other(Arc::new(e.into())))?;

    // let lua = LUA_POOL.with(|p| p.load(app_state))?;

    let lua = build_lua_engine(app_state)?;
    Ok(LuaGuard::new(lua, permit))
}
