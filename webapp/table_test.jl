using DataFrames
using Stipple
using StippleUI

sources = ["University 1", "University 2", "Hospital 1"]
conditions = ["Bipolar Disorder", "Depression", "Suicidality"]
sexes = ["Male", "Female"]
races = ["Black", "Caucasian", "Asian"]
age_groups = [
    "0 - 9",
    "10 - 19",
    "20 - 29",
    "30 - 39",
    "40 - 49",
    "50 - 59",
    "60 - 69",
    "70 - 79",
    "80 - 89",
]

WEB_TRANSPORT = Genie.WebChannels

data = DataFrames.insertcols!(dataset("datasets", "iris"))

@reactive mutable struct IrisModel <: ReactiveModel
  iris_data::R{DataTable} = DataTable(data)
  table_pagination::DataTablePagination =
    DataTablePagination(rows_per_page=50) 

end

Stipple.register_components(IrisModel)

function ui(model::IrisModel)
  page(
    model, class="container", title="Iris Flowers Clustering", head_content=Genie.Assets.favicon_support(),

    prepend = style(
    """
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
    """
    ),

    [
      heading("Iris data k-means clustering")

      row([
        cell(class="st-module", [
          h5("Iris data")
          table(:iris_data; pagination=:table_pagination, dense=true, flat=true, style="height: 350px;")
        ])
      ])
    ]
  )
end

route("/") do
  init(IrisModel(), transport = WEB_TRANSPORT) |> ui |> html
end

up(9000; async = true, server = Stipple.bootstrap())
