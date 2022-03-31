"""
TODO: Add note about workaround
"""
DBInterface.connect(::Type{LibPQ.Connection}, args...; kws...) =
    LibPQ.Connection(args...; kws...)

"""
TODO: Add note about workaround
"""
DBInterface.prepare(conn::LibPQ.Connection, args...; kws...) =
    LibPQ.prepare(conn, args...; kws...)

"""
TODO: Add note about workaround
"""
DBInterface.execute(conn::Union{LibPQ.Connection, LibPQ.Statement}, args...; kws...) =
    LibPQ.execute(conn, args...; kws...)

"""
	unzip(file::String, output_path::String)

Unzip a zipped archive.

# Arguments

- `file` - a zip archive to unzip.
- `output_path` - output directory where unzipped files are placed.

"""
function unzip(file::String, output_path::String)
    fileFullPath = isabspath(file) ? file : joinpath(pwd(), file)
    basePath = dirname(fileFullPath)
    outPath = (
        output_path == "" ? basePath :
        (isabspath(output_path) ? output_path : joinpath(pwd(), output_path))
    )
    isdir(outPath) ? "" : mkdir(outPath)
    zarchive = ZipFile.Reader(fileFullPath)
    for f in zarchive.files
        fullFilePath = joinpath(outPath, f.name)
        if (endswith(f.name, "/") || endswith(f.name, "\\"))
            mkdir(fullFilePath)
        else
            write(fullFilePath, read(f))
        end
    end
    close(zarchive)
end


"""
	download_dataset(; dataset_names = [])

# Arguments

- `dataset_names` - list of data sets to download.  Downloads all available datasets if no list is provided. Requires internet connection.

"""
function download_dataset(; dataset_names = [])
    if dataset_names |> isempty
        for dataset in DATASETS
            path = joinpath(datadir("exp_raw"), dataset.name)
            if !ispath(path)
                mkpath(path)
                dl_file = download(dataset.url, datadir(path, dataset.name * ".zip"))
                unzip(dl_file, path)
                rm(dl_file)
            else
                @warn "Path for $(dataset.name) already exists. Skipping download."
            end
        end
    else
        for name in dataset_names
            for dataset in DATASETS
                if name == dataset.name
                    path = joinpath(datadir("exp_raw"), dataset.name)
                    if !ispath(path)
                        mkpath(path)
                        dl_file =
                            download(dataset.url, datadir(path, dataset.name * ".zip"))
                        unzip(dl_file, path)
                        rm(dl_file)
                    else
                        @warn "Path for $(dataset.name) already exists. Skipping download."
                    end
                end
            end
        end
    end
end

export unzip, download_dataset
