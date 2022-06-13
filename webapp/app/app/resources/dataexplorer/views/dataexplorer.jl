    page(
        model,
        class = "container-fluid",
        title = "Study Data Explorer",
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
            heading("Baseline Data Explorer: Assessing Health Equity in Mental Healthcare Delivery Using a Federated Network Research Model")
            row(
                cell(
                    class = "container-fluid ",
                    [plot(:plot_data, layout = :layout, config = :config)],
                ),
            )
            row([
                cell(
                    class = "st-module",
                    [
                        h5("Disease Prevalence Data")
                        table(
                            :study_data;
                            pagination = :table_pagination,
                            dense = true,
                            flat = true,
                            style = "height: 200px;",
                            loading = :data_loading,
                        )
                    ],
                ),
            ],)
            row(
                cell(
                    class = "st-module",
                    [
                        checkbox(label = "Bipolar Disorder", fieldname = :valone),
                        checkbox(label = "Depression", fieldname = :valtwo),
                        checkbox(label = "Suicidality", fieldname = :valthree),
                    ],
                ),
            )
            row(
                cell(
                    class = "st-module",
                    [
                        checkbox(label = "White", fieldname = :valfour),
                        checkbox(label = "Black or African American", fieldname = :valfive),
                        checkbox(label = "Other Race", fieldname = :valsix),
                        checkbox(label = "Asian", fieldname = :valseven),
                        checkbox(label = "American Indian or Alaska Native", fieldname = :valeight),
                    ],
                ),
            )
            row(
                cell(
                    class = "st-module",
                    [
                        checkbox(label = "Male", fieldname = :valnine),
                        checkbox(label = "Female", fieldname = :valten),
                    ],
                ),
            )
        ],
    )
