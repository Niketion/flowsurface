// Camera/Params + shared helpers come from common.wgsl

struct VertexInput {
    @location(0) local_pos: vec2<f32>,
};

struct VertexOutput {
    @builtin(position) pos: vec4<f32>,
    @location(0) uv: vec2<f32>,
};

@group(1) @binding(0)
var heatmap_rg: texture_2d<u32>;

@vertex
fn vs_main(input: VertexInput) -> VertexOutput {
    var out: VertexOutput;
    out.pos = vec4<f32>(input.local_pos * 2.0, 0.0, 1.0);
    out.uv = input.local_pos + vec2<f32>(0.5, 0.5);
    return out;
}

@fragment
fn fs_main(input: VertexOutput) -> @location(0) vec4<f32> {
    let viewport = camera.b.xy;

    let screen_xy = input.uv * viewport;
    let world = screen_to_world(screen_xy);

    if (world.x > 0.0) {
        return vec4<f32>(0.0);
    }

    let col_w = max(params.grid.x, 1e-12);
    let row_h = max(params.grid.y, 1e-12);
    let steps_per = max(params.grid.z, 1.0);
    let steps_per_i = max(i32(round(steps_per)), 1);

    let tex_w_u = u32(params.heatmap_tex.x);
    let tex_h_u = u32(params.heatmap_tex.y);
    if (tex_w_u < 1u || tex_h_u < 1u) {
        return vec4<f32>(0.0);
    }

    let steps_at_y = i32(floor((-world.y) / max(row_h, 1e-12)));
    let y_bin_rel = steps_at_y / steps_per_i;
    let y_start_bin = i32(params.heatmap_map.y);
    let yi = y_bin_rel - y_start_bin;
    if (yi < 0 || u32(yi) >= tex_h_u) {
        return vec4<f32>(0.0);
    }

    let latest_bucket_rel = i32(params.heatmap_map.x);
    let render_rel = i32(floor(params.origin.x + (world.x / col_w)));

    var bucket_rel_from_latest = render_rel - latest_bucket_rel;
    bucket_rel_from_latest = min(bucket_rel_from_latest, 0);

    let oldest = -i32(tex_w_u) + 1;
    if (bucket_rel_from_latest < oldest) {
        return vec4<f32>(0.0);
    }

    let latest_x_ring = i32(u32(params.heatmap_map.w));
    let tex_w_mask = i32(u32(params.heatmap_tex.z));
    let xi = (latest_x_ring + bucket_rel_from_latest) & tex_w_mask;

    let inv_qty_scale = params.heatmap_tex.w;

    let v = textureLoad(heatmap_rg, vec2<i32>(xi, yi), 0);
    let bid_qty = f32(v.x) * inv_qty_scale;
    let ask_qty = f32(v.y) * inv_qty_scale;

    let max_depth = max(params.depth.x, 1e-12);
    let alpha_min = params.depth.y;
    let alpha_max = params.depth.z;

    let total_qty = bid_qty + ask_qty;
    let t = clamp(total_qty / max_depth, 0.0, 1.0);

    if (total_qty == 0.0) {
        return vec4<f32>(0.0);
    }

    let a = alpha_min + t * (alpha_max - alpha_min);

    // Viridis approximation
    let c0 = vec3<f32>(0.2777273272234177, 0.005407344544966578, 0.3340998053353061);
    let c1 = vec3<f32>(0.1050930431085774, 1.404613529898575, 1.384590162594685);
    let c2 = vec3<f32>(-0.3308618287255563, 0.214847559468213, 0.09509516302823659);
    let c3 = vec3<f32>(-4.634230498983486, -5.799100973351585, -19.33244095627987);
    let c4 = vec3<f32>(6.228269936347081, 14.17993336680509, 56.69055260068105);
    let c5 = vec3<f32>(4.776384997670288, -13.74514537774601, -65.35303263337234);
    let c6 = vec3<f32>(-5.435455855934631, 4.645852612178535, 26.3124352495832);

    let color = c0 + t * (c1 + t * (c2 + t * (c3 + t * (c4 + t * (c5 + t * c6)))));
    let clamped_color = clamp(color, vec3<f32>(0.0), vec3<f32>(1.0));

    let fade = fade_factor(world.x);
    return vec4<f32>(clamped_color * a * fade, a * fade);
}
