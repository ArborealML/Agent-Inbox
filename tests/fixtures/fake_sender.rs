use std::env;
fn main() {
    let args:Vec<_>=env::args().skip(1).collect();
    assert_eq!(&args[..4],["queue","--thread","test-worker","--message"]);
    assert_eq!(args.len(),5);
    std::fs::write(env::var_os("CAPTURE").unwrap(),&args[4]).unwrap();
}
