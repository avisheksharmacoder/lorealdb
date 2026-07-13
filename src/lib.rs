use pyo3::prelude::*;

// declare new db_engine modules.
pub mod db_engine;
pub mod db_engine_write;

// import DBEngines to use.
use db_engine::DBEngine;
use db_engine_write::DBEngineWriteOptimized;

// lorealdb module declaration.
#[pymodule]
fn lorealdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = m.add_class::<DBEngine>()?;
    let _ = m.add_class::<DBEngineWriteOptimized>()?;
    Ok(())
}
