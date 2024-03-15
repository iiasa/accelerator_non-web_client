import typing
import typer
import json
import os
import subprocess
import csv
import uuid
import itertools
from typing import Optional
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import print

from jsonschema import validate as jsonschema_validate
from jsonschema.exceptions import ValidationError, SchemaError


if typing.TYPE_CHECKING:
    from accli.AcceleratorTerminalCliProjectService import AcceleratorTerminalCliProjectService

def lower_rows(iterator):
    # return itertools.chain([next(iterator).lower()], iterator)
    for item in iterator:
        yield item.lower()

class CsvRegionalTimeseriesValidator():
    def __init__(
        self,
        *,
        project_slug,
        dataset_template_slug,
        input_filepath,
        project_service: 'AcceleratorTerminalCliProjectService',
        csv_fieldnames: Optional[typing.List[str]]=None,
        
    ):
        self.project_slug = project_slug
        self.project_service = project_service

        self.dataset_template_slug = dataset_template_slug

        self.input_filepath = input_filepath

        self.csv_fieldnames = csv_fieldnames

        

        self.errors = dict()
    
    
    def get_map_documents(self, field_name):
        map_documents = self.rules.get(f'map_{field_name}')
        return map_documents


    def init_validation_metadata(self):
        self.validation_metadata = {
            self.time_dimension.lower(): {
                "min_value": float('+inf'),
                "max_value": float('-inf')
            }
        }


    def set_csv_regional_validation_rules(self):
        
        dataset_template_details = self.project_service.get_dataset_template_details(
            self.project_slug, 
            self.dataset_template_slug
        )
        
        self.rules =  dataset_template_details.get('rules')

        assert self.rules, \
            f"No dataset template rules found for dataset_template id: \
                {self.dataset_template_id}"
        
        self.time_dimension = self.rules['root_schema_declarations']['time_dimension']
        self.value_dimension = self.rules['root_schema_declarations']['value_dimension']

        self.region_dimension = self.rules['root_schema_declarations']['region_dimension']

        # specific to regional timeseries
        self.region_layer_dimension = self.rules['root_schema_declarations']['region_layer_map_key']

    def validate_row_data(self, row):
        try:
            jsonschema_validate(
                self.rules.get('root'),
                row
            )

        except SchemaError as schema_error:
           
            raise ValueError(
                f"Schema itself is not valid with template id. Template id: {self.dataset_template_id}. Original exception: {str(schema_error)}"
            )
        except ValidationError as validation_error:
            raise ValueError(
                f"Invalid data. Template id: {self.dataset_template_id}. Data: {str(validation_error)}. Original exception: {str(validation_error)}"
            )
        

        for key in self.rules['root']['properties']:

            if key == self.time_dimension.lower():
                if float(row[key]) < self.validation_metadata[
                    self.time_dimension.lower()
                ]["min_value"]:
                    self.validation_metadata[
                        self.time_dimension.lower()
                    ]["min_value"] = float(row[key])

                if float(row[key]) > self.validation_metadata[
                    self.time_dimension.lower()
                ]["max_value"]:
                    self.validation_metadata[
                        self.time_dimension.lower()
                    ]["max_value"] = float(row[key])
                
                continue

            if key == self.value_dimension:
                continue


            map_documents = self.get_map_documents(key)

            if map_documents:
                if row[key] not in map_documents:
                    raise ValueError(f"'{row[key]}' must be one of {map_documents.keys()}" )
                
        
            if self.validation_metadata.get(key):
                if len(self.validation_metadata[key]) <= 200: #limit harvest
                    self.validation_metadata[key].add(row[key])

            else:
                self.validation_metadata[key] = set([row[key]])


        extra_template_validators = self.rules.get('template_validators')

        if extra_template_validators and extra_template_validators != 'not defined':
                
            for row_key in extra_template_validators.keys():
                lhs = row[row_key]

                condition_object = extra_template_validators[row_key]

                for condition in condition_object.keys():
                
                    rhs_value_pointer = condition_object[condition]

                    rhs = None
                    for pointer in rhs_value_pointer:
                        if pointer.startswith('&'):
                            rhs = self.rules[pointer[1:]]
                        elif pointer.startswith('{') and pointer.endswith('}'):
                            rhs = rhs[row[pointer[1:-1]]]

                        else:
                            rhs = rhs[pointer]


                    if condition == 'value_equals':
                        if lhs.lower() != rhs.lower():
                            raise ValueError(
                                f'{lhs} in {row_key} column must be equal to {rhs}.'
                            )
                    
                    if condition == 'is_subset_of_map':
                        if not lhs.lower() in rhs.lower():
                            raise ValueError(
                                f'{lhs} in {row_key} column must be member of {rhs}.'
                            )

        return row          
    def get_validated_rows(self, filepath):
        with open(filepath) as csvfile:
        # with open(self.temp_downloaded_filepath) as csvfile:
            reader = csv.DictReader(
                lower_rows(csvfile), 
                fieldnames=self.csv_fieldnames, 
                restkey='restkeys', 
                restval='restvals'
            )

            for row in reader:
                row.pop('restkeys', None)
                row.pop('restvals', None)

                try:
                    row = self.validate_row_data(row)
                    row[self.region_layer_dimension] = self.rules[f'map_{self.region_dimension}'][row[self.region_dimension]]['region_layer']
                except Exception as err:
                    if len(self.errors) <= 50:
                        self.errors[str(err)] = str(row)

                yield row
       
                
    def __call__(self):
        
        self.set_csv_regional_validation_rules()
        self.init_validation_metadata()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            progress.add_task(description="[bold cyan]Validating dataset. Please wait...", total=None)
        
            for row in self.get_validated_rows(self.input_filepath):
                pass
        
        
        if self.errors:
            for key in self.errors:
                print(f"[bold red]Invalid data:[/bold red] {self.errors[key]}")
                print(f"[bold red]Error:[/bold red] {key}")
            print("[bold red]Data is not valid against selected template. ⚠️⚠️")
            typer.Exit(1)
        else:
            print("[bold green]Data validated against selected template.[/bold green] 🎉🎉")
            typer.Exit(0)



        