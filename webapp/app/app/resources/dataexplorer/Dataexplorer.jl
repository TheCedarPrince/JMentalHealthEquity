module Dataexplorer

using CSV
using DataFrames
using Genie
using Genie.Renderer.Html
using Stipple
using StippleUI
using StipplePlotly

prevalence = CSV.read("data/exp_pro/baseline/prevalence.csv", DataFrame)
for col in names(prevalence)
	prevalence[:,col]= [ ismissing(x) ? 0 : x for x in prevalence[:,col] ]
end

pd(x, y, name) =
    PlotData(x = x, y = y, plot = StipplePlotly.Charts.PLOT_TYPE_BAR, name = name)

export StudyModel

@reactive mutable struct StudyModel <: ReactiveModel
    study_data::R{DataTable} = DataTable(prevalence)
    table_pagination::DataTablePagination = DataTablePagination(rows_per_page = 3)

    plot_data::R{Vector{PlotData}} =
        [pd(1:9, gdf.prevalence, gdf.condition |> first) for gdf in groupby(prevalence, :condition)]

    layout::R{PlotLayout} = PlotLayout(
        plot_bgcolor = "#333",
        title = PlotLayoutTitle(text = "Disease Prevalence versus Age Group", font = Font(24)),
	xaxis = [PlotLayoutAxis(xy = "x", title_text = "Age Groups", font = Font(14), tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9], ticktext = ["0 - 9", "10 - 19", "20 - 29", "30 - 39", "40 - 49", "50 - 59", "60 - 69", "70 - 79", "80 - 89"])],
	yaxis = [PlotLayoutAxis(xy = "y", title_text = "Prevalence", font = Font(14))]
    )

    config::R{PlotConfig} = PlotConfig()

    # Condition checkboxes
    valone::R{Bool} = false
    valtwo::R{Bool} = true
    valthree::R{Bool} = false

    # Race checkboxes
    valfour::R{Bool} = false
    valfive::R{Bool} = true
    valsix::R{Bool} = false
    valseven::R{Bool} = false
    valeight::R{Bool} = false
    
    # Gender checkboxes
    valnine::R{Bool} = false
    valten::R{Bool} = true

    condition_cols::R{Vector} = ["Depression"]
    race_cols::R{Vector} = ["Black or African American"]
    gender_cols::R{Vector} = ["Female"]

    data_loading::R{Bool} = false

end

function handler(model)

    #####################
    # CONDITION FILTERING
    #####################
    on(model.valone) do val
        if val == true
            push!(model.condition_cols.o.val, "Bipolar Disorder")
        else
            deleteat!(
                model.condition_cols.o.val,
                findall(x -> x == "Bipolar Disorder", model.condition_cols.o.val),
            )
        end
    end
    on(model.valtwo) do val
        if val == true
            push!(model.condition_cols.o.val, "Depression")
        else
            deleteat!(model.condition_cols.o.val, findall(x -> x == "Depression", model.condition_cols.o.val))
        end
    end
    on(model.valthree) do val
        if val == true
            push!(model.condition_cols.o.val, "Suicidality")
        else
            deleteat!(model.condition_cols.o.val, findall(x -> x == "Suicidality", model.condition_cols.o.val))
        end
    end

    ################
    # RACE FILTERING
    ################
    on(model.valfour) do val
        if val == true
            push!(model.race_cols.o.val, "White")
        else
            deleteat!(
                model.race_cols.o.val,
                findall(x -> x == "White", model.race_cols.o.val),
            )
        end
    end
    on(model.valfive) do val
        if val == true
            push!(model.race_cols.o.val, "Black or African American")
        else
            deleteat!(model.race_cols.o.val, findall(x -> x == "Black or African American", model.race_cols.o.val))
        end
    end
    on(model.valsix) do val
        if val == true
            push!(model.race_cols.o.val, "Other Race")
        else
            deleteat!(model.race_cols.o.val, findall(x -> x == "Other Race", model.race_cols.o.val))
        end
    end
    on(model.valseven) do val
        if val == true
            push!(model.race_cols.o.val, "Asian")
        else
            deleteat!(model.race_cols.o.val, findall(x -> x == "Asian", model.race_cols.o.val))
        end
    end
    on(model.valeight) do val
        if val == true
            push!(model.race_cols.o.val, "American Indian or Alaska Native")
        else
            deleteat!(model.race_cols.o.val, findall(x -> x == "American Indian or Alaska Native", model.race_cols.o.val))
        end
    end
    
    ##################
    # GENDER FILTERING
    ##################
    on(model.valnine) do val
        if val == true
            push!(model.gender_cols.o.val, "Male")
        else
            deleteat!(model.gender_cols.o.val, findall(x -> x == "Male", model.gender_cols.o.val))
        end
    end
    on(model.valten) do val
        if val == true
            push!(model.gender_cols.o.val, "Female")
        else
            deleteat!(model.gender_cols.o.val, findall(x -> x == "Female", model.gender_cols.o.val))
        end
    end
    
    model
end

function filterdata(model)
    model.data_loading[] = true
    model.study_data[] = DataTable(
        filter(
            row ->
                in(row.condition, model.condition_cols.o.val) &&
                    in(row.race_concept_id, model.race_cols.o.val)
		&&
		    in(row.gender_concept_id, model.gender_cols.o.val),
            prevalence,
        ),
    )
    model.plot_data[] = [
        pd(1:9, gdf.prevalence, gdf.condition |> first) for gdf in groupby(
            filter(
                row ->
                    in(row.condition, model.condition_cols.o.val) &&
                        in(row.race_concept_id, model.race_cols.o.val)
			&&
		    in(row.gender_concept_id, model.gender_cols.o.val),
                prevalence,
            ),
            :condition,
        )
    ]
    model.data_loading[] = false
end

function model_factory()
  model = StudyModel |> init |> handler

    onany(model.valone, model.valtwo, model.valthree, model.valfour, model.valfive, model.valsix, model.valseven, model.valeight, model.valnine, model.valten) do (_...)
        filterdata(model)
    end

  model
end

end
