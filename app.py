import streamlit as st
from query_full_knowledge_graph import generate_sparql_query_with_llm, query_knowledge_graph
from explore_datasets import explore_dataset_via_ui


# Streamlit App
st.set_page_config(page_title="Knowledge Graph Dataset Explorer", layout="wide")


def main():
    # Page title
    st.title("Knowledge Graph Dataset Explorer")
    st.write("Search datasets and analyze them using the power of LLMs!")

    # Step 1: User query for the knowledge graph
    st.header("Step 1: Search Datasets")
    user_query = st.text_input("Enter your query (e.g., 'Show me datasets about air pollution in PDF'): ")

    if st.button("Search"):
        if user_query:
            with st.spinner("Generating SPARQL query and fetching datasets..."):
                sparql_query = generate_sparql_query_with_llm(user_query)
                if sparql_query:
                    results = query_knowledge_graph(sparql_query)
                    if results:
                        st.success("Datasets found!")
                        st.write("Here are the datasets:")

                        # Store results in session state
                        st.session_state["datasets"] = results
                        # Initialize expander states
                        st.session_state["expander_state"] = [False] * len(results)
                    else:
                        st.warning("No datasets found for the query.")
                else:
                    st.error("Failed to generate SPARQL query.")
        else:
            st.warning("Please enter a query to search.")

    # Step 2: Display and analyze datasets
    if "datasets" in st.session_state:
        st.header("Step 2: Explore and Analyze Datasets")
        datasets = st.session_state["datasets"]

        for idx, result in enumerate(datasets, start=1):
            st.write(f"### Dataset {idx}")
            st.write(f"**Title:** {result.get('title', 'N/A')}")
            st.write(f"**Description:** {result.get('description', 'N/A')}")
            st.write(f"**Format:** {result.get('format', 'N/A')}")
            st.write(f"**Publisher:** {result.get('publisher', 'N/A')}")
            st.write(f"**URL:** [Link]({result.get('url', 'N/A')})")

            # Manage the expander state
            expander_key = f"expander_{idx}"
            if "expander_state" not in st.session_state:
                st.session_state["expander_state"] = [False] * len(datasets)

            # Set the state of the current expander based on session state
            expander_open = st.session_state["expander_state"][idx - 1]

            # Create an expander for each dataset
            with st.expander(f"Explore Dataset {idx}", expanded=expander_open):
                # Input for user question
                user_question_key = f"user_question_{idx}"
                if user_question_key not in st.session_state:
                    st.session_state[user_question_key] = ""

                # Text area for the user's question
                user_question = st.text_area(
                    f"Ask a question about Dataset {idx}:",
                    key=user_question_key,
                )

                # Analyze Dataset button
                analyze_button_key = f"analyze_{idx}"
                if st.button(f"Analyze Dataset {idx}", key=analyze_button_key):
                    if result.get("url") and user_question:
                        # Keep the expander open while processing
                        st.session_state["expander_state"][idx - 1] = True

                        # Create a placeholder for real-time response updates
                        response_placeholder = st.empty()
                        response_placeholder.markdown("### Analyzing dataset...\n")

                        # Stream the response from the LLM in real-time
                        response_text = ""
                        for fragment in explore_dataset_via_ui(result["url"], user_question):
                            response_text += fragment  # Append each fragment to the response text
                            response_placeholder.markdown(response_text)  # Update the placeholder

                        # Mark analysis as complete
                        st.success("Analysis complete!")

                # Ensure the expander stays open after processing
                st.session_state["expander_state"][idx - 1] = True


if __name__ == "__main__":
    main()
