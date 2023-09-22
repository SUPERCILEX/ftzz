use std::{fs::File, io::Write, ptr};

const MAX_CACHE_SIZE: usize = 1000;

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let mut cache = File::create(format!("{out_dir}/file_name_cache.bin")).unwrap();

    let mut buf = [0u8; 3000];
    let mut num_buf = itoa::Buffer::new();

    let buf_ptr = buf.as_mut_ptr().cast::<u8>();
    for i in 0..MAX_CACHE_SIZE {
        let bytes = num_buf.format(i).as_bytes();
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), buf_ptr.add(i * 3), bytes.len());
        }
    }

    cache.write_all(&buf).unwrap();
}
