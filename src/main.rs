use plotters::prelude::*;
use postgres::{Client, NoTls};

use std::env;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::time::{Duration, Instant, SystemTime};

type PerfResults = Vec<Duration>;

fn simple_file_insert_log(file_name: &str) {
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_name)
        .unwrap();

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();

    match writeln!(file, "user made a request|{}", now.as_nanos()) {
        Err(e) => println!("{e}"),
        _ => (),
    };
}

fn measure_logs_files(iterations: usize) -> PerfResults {
    match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("my_simple_log_file")
    {
        Ok(_) => println!("Log file open. Testing log file"),
        Err(e) => {
            panic!("Log file could not be opened, cancelling test. {e}");
        }
    };
    let mut results = vec![];

    for i in 0..iterations {
        let before = Instant::now();
        simple_file_insert_log("my_simple_log_file");
        results.push(before.elapsed());

        if i % 10000 == 0 {
            println!("{i}");
        }
    }

    return results;
}

fn rdbms_insert_log(client: &mut Client) {
    client.execute(
        "INSERT INTO log_table (text, time)  VALUES ('user made a request', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')));", 
           &[]
       ).unwrap();
}

fn measure_logs_rdbms(iterations: usize) -> PerfResults {
    let mut client = Client::connect("postgresql://ethancarlsson@localhost/log_db", NoTls).unwrap();

    client
        .batch_execute(
            "
    CREATE TABLE IF NOT EXISTS log_table (
        id      SERIAL PRIMARY KEY,
        text    TEXT NOT NULL,
        time    INTEGER
    );

    DELETE FROM log_table;
",
        )
        .unwrap();

    let mut results = vec![];

    for i in 0..iterations {
        let before = Instant::now();
        rdbms_insert_log(&mut client);
        results.push(before.elapsed());

        if i % 10000 == 0 {
            println!("{i}");
        }
    }

    return results;
}

fn measure_logs_sqlite_sync_off(iterations: usize) -> PerfResults {
    let conn = sqlite::Connection::open("sync_off").unwrap();
    conn.execute(
        "
                  CREATE TABLE IF NOT EXISTS log_table (
                        text    TEXT NOT NULL,
                        time    INTEGER
                      );
                  DELETE FROM log_table;
                  PRAGMA synchronous=OFF;
                  ",
    )
    .unwrap();

    let mut results = vec![];
    for i in 0..iterations {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let before = Instant::now();

        conn.execute(format!(
            "INSERT INTO log_table (text, time)  VALUES ('user made a request', {});",
            now.as_secs()
        ))
        .unwrap();

        results.push(before.elapsed());

        if i % 10000 == 0 {
            println!("{i}");
        }
    }

    results
}

fn measure_logs_rdbms_no_id(iterations: usize) -> PerfResults {
    let mut client = Client::connect("postgresql://ethancarlsson@localhost/log_db", NoTls).unwrap();

    client
        .batch_execute(
            "
    CREATE TABLE IF NOT EXISTS log_table (
        text    TEXT NOT NULL,
        time    INTEGER
    );

    DELETE FROM log_table;
",
        )
        .unwrap();

    let mut results = vec![];

    for i in 0..iterations {
        let before = Instant::now();
        rdbms_insert_log(&mut client);
        results.push(before.elapsed());

        if i % 10000 == 0 {
            println!("{i}");
        }
    }

    return results;
}

fn generate_plot(perf_results: &PerfResults, plot_name: &str) {
    let nanos = perf_results.iter().map(|duration| duration.as_nanos());
    let name = format!("images/{plot_name}.png");
    let root_area = BitMapBackend::new(&name, (1200, 800)).into_drawing_area();

    root_area.fill(&WHITE).unwrap();

    let max_height: u128 = 500000;

    let mut ctx = ChartBuilder::on(&root_area)
        .set_label_area_size(LabelAreaPosition::Left, 80)
        .set_label_area_size(LabelAreaPosition::Bottom, 80)
        .caption(format!("{plot_name}"), ("sans-serif", 40))
        .build_cartesian_2d(0..nanos.len(), 0..max_height)
        .unwrap();

    ctx.configure_mesh().draw().unwrap();

    let series = LineSeries::new(nanos.enumerate(), &GREEN);

    ctx.draw_series(series).unwrap();
}

fn generate_comparison_plot(
    results_rdbms: &PerfResults,
    results_file: &PerfResults,
    other: &PerfResults,
    other_name: &str,
) {
    let name = format!("images/rdbms_file_{other_name}.png");
    let root_area = BitMapBackend::new(&name, (1200, 800)).into_drawing_area();

    root_area.fill(&WHITE).unwrap();

    let nanos_rdbms = results_rdbms.iter().map(|duration| duration.as_nanos());
    let nanos_other = other.iter().map(|duration| duration.as_nanos());
    let nanos_file = results_file.iter().map(|duration| duration.as_nanos());

    let max_height: u128 = 500000;
    let mut ctx = ChartBuilder::on(&root_area)
        .set_label_area_size(LabelAreaPosition::Left, 80)
        .set_label_area_size(LabelAreaPosition::Right, 20)
        .set_label_area_size(LabelAreaPosition::Bottom, 80)
        .caption(
            format!("rdbms vs file vs  rdbms {other_name} for logging"),
            ("sans-serif", 40),
        )
        .build_cartesian_2d(0..nanos_rdbms.len(), 0..max_height)
        .unwrap();

    ctx.configure_mesh().draw().unwrap();

    let series_rdbms = LineSeries::new(nanos_rdbms.clone().enumerate(), &GREEN);
    let series_other = LineSeries::new(nanos_other.clone().enumerate(), &BLUE);
    let series_file = LineSeries::new(nanos_file.clone().enumerate(), &RED);

    ctx.draw_series(series_rdbms)
        .unwrap()
        .label(format!(
            "rdbms avg {}ns",
            nanos_rdbms.sum::<u128>() / u128::try_from(results_rdbms.len()).unwrap()
        ))
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &GREEN));
    ctx.draw_series(series_other)
        .unwrap()
        .label(format!(
            "{other_name} avg {}ns",
            nanos_other.sum::<u128>() / u128::try_from(other.len()).unwrap()
        ))
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &BLUE));
    ctx.draw_series(series_file)
        .unwrap()
        .label(format!(
            "files avg {}ns",
            nanos_file.sum::<u128>() / u128::try_from(results_file.len()).unwrap()
        ))
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED));

    ctx.configure_series_labels()
        .border_style(&BLACK)
        .background_style(&WHITE.mix(0.8))
        .draw()
        .unwrap();
}

fn main() {
    let iterations = env::args()
        .nth(1)
        .unwrap_or("5".to_string())
        .parse::<usize>()
        .unwrap_or(5);

    let results_rdbms_no_id = measure_logs_rdbms_no_id(iterations);
    let results_rdbms = measure_logs_rdbms(iterations);
    let results_file = measure_logs_files(iterations);
    let results_sqlite_sync_off = measure_logs_sqlite_sync_off(iterations);

    generate_plot(&results_rdbms, "rdbms");
    generate_plot(&results_file, "file");

    generate_comparison_plot(
        &results_rdbms,
        &results_file,
        &results_rdbms_no_id,
        "results_rdbms_no_id",
    );

    generate_comparison_plot(
        &results_rdbms,
        &results_file,
        &results_sqlite_sync_off,
        "sqlite_sync_off",
    )
}
