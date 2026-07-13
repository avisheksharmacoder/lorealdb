use pyo3::prelude::*;

// declare new db_engine module.
pub mod db_engine;

// import DBEngine to use.
use db_engine::DBEngine;

// lorealdb module declaration.
#[pymodule]
fn lorealdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = m.add_class::<DBEngine>();
    Ok(())
}
