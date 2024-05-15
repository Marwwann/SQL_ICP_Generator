import os
import CallFusion
import helper


def generateScripts(save_directory, instance, username, password, total_string, elements_list):
    # Prepare the main query
    main_query = helper.getMainQuery(total_string)

    # Run the main query
    main_df = CallFusion.getResult(instance, username, password, main_query)

    # Loop over each entered element entry
    for element_name in elements_list:
        # Filter the dataframe to the current element entry only
        working_df = main_df[main_df["REPORTING_NAME"] == element_name]
        # The passed element entry may be invalid, so it won't have any records in the dataframe
        if working_df.shape[0] == 0:
            pass
        else:
            # Get the element type ID
            element_type_id = working_df.iloc[0, 1]

            # Construct the create scenario query
            main_create_query = ", ".join(working_df["MAIN_CREATE_QUERY"].values.flatten().tolist())
            # Construct the update scenario query
            main_update_query = ", ".join(working_df["MAIN_UPDATE_QUERY"].values.flatten().tolist())
            # Construct the delete scenario query
            main_delete_query = ", ".join(working_df["MAIN_DELETE_QUERY"].values.flatten().tolist())

            # Construct the SELECT statement for the three scenarios
            select_create_query = helper.createSelectQuery(element_type_id)
            select_update_query = helper.updateSelectQuery(element_type_id)
            select_delete_query = helper.deleteSelectQuery()

            # Construct the FROM statement for the three scenarios
            from_create_query = helper.createFromQuery(working_df)
            from_update_query = helper.updateFromQuery(working_df)
            from_delete_query = helper.deleteFromQuery(element_type_id)

            # Concatenate the complete query
            create_query = select_create_query + main_create_query + from_create_query
            update_query = select_update_query + main_update_query + from_update_query
            delete_query = select_delete_query + main_delete_query + from_delete_query

            # Check if the PARAMETER.PersonId bind variable is in the query and replace it with the person ID query
            # in the 3 scenarios
            if create_query.find(":{PARAMETER.PersonId}") != -1:
                create_query = create_query.replace(":{PARAMETER.PersonId}", helper.getPersonId())

            if update_query.find(":{PARAMETER.PersonId}") != -1:
                update_query = update_query.replace(":{PARAMETER.PersonId}", helper.getPersonId())

            if delete_query.find(":{PARAMETER.PersonId}") != -1:
                delete_query = delete_query.replace(":{PARAMETER.PersonId}", helper.getPersonId())

            # Check that there is a folder with the element name, if not then create it
            if os.path.exists(f"{save_directory}/{element_name}"):
                continue
            else:
                os.mkdir(f"{save_directory}/{element_name}")

            # Save the SQL files
            f = open(f"{save_directory}/{element_name}/{element_name}_CREATE_QUERY.sql", 'a')
            f.write(create_query)
            f.close()

            f = open(f"{save_directory}/{element_name}/{element_name}_UPDATE_QUERY.sql", 'a')
            f.write(update_query)
            f.close()

            f = open(f"{save_directory}/{element_name}/{element_name}_DELETE_QUERY.sql", 'a')
            f.write(delete_query)
            f.close()
