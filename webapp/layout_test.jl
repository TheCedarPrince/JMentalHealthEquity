using DataFrames
using Stipple
using StippleUI

import RDatasets: dataset

WEB_TRANSPORT = Genie.WebChannels

data = DataFrames.insertcols!(dataset("datasets", "iris"))

@reactive mutable struct IrisModel <: ReactiveModel
    iris_data::R{DataTable} = DataTable(data)
    table_pagination::DataTablePagination = DataTablePagination(rows_per_page = 50)

    valone::R{Bool} = true
    valtwo::R{Bool} = true
    valthree::R{Bool} = true
    valfour::R{Bool} = true
    valfive::R{Bool} = true
    cols::R{Vector} = ["SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species"]

    data_loading::R{Bool} = false

end

Stipple.register_components(IrisModel)

function handler(model)
    on(model.valone) do val
        if val == true
            push!(model.cols.o.val, "SepalLength")
        else
            deleteat!(model.cols.o.val, findall(x -> x == "SepalLength", model.cols.o.val))
        end
    end
    on(model.valtwo) do val
        if val == true
            push!(model.cols.o.val, "SepalWidth")
        else
            deleteat!(model.cols.o.val, findall(x -> x == "SepalWidth", model.cols.o.val))
        end
    end
    on(model.valthree) do val
        if val == true
            push!(model.cols.o.val, "PetalLength")
        else
            deleteat!(model.cols.o.val, findall(x -> x == "PetalLength", model.cols.o.val))
        end
    end
    on(model.valfour) do val
        if val == true
            push!(model.cols.o.val, "PetalWidth")
        else
            deleteat!(model.cols.o.val, findall(x -> x == "PetalWidth", model.cols.o.val))
        end
    end
    on(model.valfive) do val
        if val == true
            push!(model.cols.o.val, "Species")
        else
            deleteat!(model.cols.o.val, findall(x -> x == "Species", model.cols.o.val))
        end
    end
    model
end

function filterdata(model)
    model.data_loading[] = true
    model.iris_data[] = DataTable(data[:, model.cols.o.val])
    model.data_loading[] = false
end

function ui(model::IrisModel)

    onany(
        model.valone,
        model.valtwo,
        model.valthree,
        model.valfour,
        model.valfive,
    ) do (_...)
        filterdata(model)
    end

    page(
        model,
        class = "container",
        title = "Iris Flowers Clustering",
        head_content = Genie.Assets.favicon_support(),
        prepend = style("""
                        tr:nth-child(even) {
                          background: #F8F8F8 !important;
                        }

                        .st-module {
                          background-color: #FFF;
                          border-radius: 2px;
                          box-shadow: 0px 4px 10px rgba(0, 0, 0, 0.04);
                        }

                        .stipple-core .st-module > h5,
                        .stipple-core .st-module > h6 {
                          border-bottom: 0px !important;
                        }
                        """),
        [
            heading("Iris data k-means clustering")
            row([
                cell(
                    class = "st-module",
                    [
                        h5("Iris data")
                        table(
                            :iris_data;
                            pagination = :table_pagination,
                            dense = true,
                            flat = true,
                            style = "height: 350px;",
                            loading = :data_loading,
                        )
                    ],
                ),
            ],)
            row(
                cell(
                    class = "st-module",
                    [
                        checkbox(label = "SepalLength", fieldname = :valone, dense = true),
                        checkbox(label = "SepalWidth", fieldname = :valtwo, dense = true),
                        checkbox(
                            label = "PetalLength",
                            fieldname = :valthree,
                            dense = true,
                        ),
                        checkbox(label = "PetalWidth", fieldname = :valfour, dense = true),
                        checkbox(label = "Species", fieldname = :valfive, dense = true),
                    ],
                ),
            )
        ],
    )
end

my_model = handler(init(IrisModel, transport = WEB_TRANSPORT))

route("/") do
    ui(my_model) |> html
end

up(9000; async = true, server = Stipple.bootstrap())
