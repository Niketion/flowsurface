use gif::{DisposalMethod, Encoder, Frame, Repeat};
use std::{fs::File, path::Path};

const WIDTH: u16 = 48;
const HEIGHT: u16 = 16;
const CENTERS: [(i32, i32); 3] = [(8, 8), (24, 8), (40, 8)];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = Path::new("assets/ui");
    std::fs::create_dir_all(output_dir)?;
    write_spinner(output_dir.join("loading-dots.gif"), [142, 142, 142])?;
    Ok(())
}

fn write_spinner(path: impl AsRef<Path>, color: [u8; 3]) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(path)?;
    let palette = [0, 0, 0, color[0], color[1], color[2]];
    let mut encoder = Encoder::new(&mut file, WIDTH, HEIGHT, &palette)?;
    encoder.set_repeat(Repeat::Infinite)?;

    for active in 0..3 {
        let mut pixels = vec![0; usize::from(WIDTH * HEIGHT)];
        for (dot, &(cx, cy)) in CENTERS.iter().enumerate() {
            let radius = if dot == active { 5 } else { 3 };
            for y in 0..i32::from(HEIGHT) {
                for x in 0..i32::from(WIDTH) {
                    if (x - cx).pow(2) + (y - cy).pow(2) <= radius * radius {
                        pixels[(y as usize * usize::from(WIDTH)) + x as usize] = 1;
                    }
                }
            }
        }

        let mut frame = Frame::from_indexed_pixels(WIDTH, HEIGHT, pixels, Some(0));
        frame.delay = 12;
        frame.dispose = DisposalMethod::Background;
        encoder.write_frame(&frame)?;
    }
    Ok(())
}
