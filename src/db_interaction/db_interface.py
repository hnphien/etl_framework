from sqlalchemy import (Column, Integer, MetaData, String, Table,
                        create_engine, update)
from sqlalchemy.orm import sessionmaker


class TalbeResources():
    """
    The class has several  methods that allow you to perform common operations on a database table,
    including adding new records, updating existing records, and deleting records
    """

    def __init__(self, conn: str, table_name: str):
        #  init session
        engine = create_engine(conn, echo=True)
        Session = sessionmaker(bind=engine)
        session = Session()

        # init table
        metadata = MetaData(bind=engine)
        metadata.reflect()
        table = Table(
            table_name,
            metadata,
            # autoload=True)
            # uncomment if you want to create table (dev env only)
            Column('id', Integer, primary_key=True),
            Column('file_name', String),
            Column('file_root', String),
            Column('file_size', String),
            Column('file_creation_date', String),
            Column('file_extension', String),
            extend_existing=True
        )
        metadata.create_all(engine)

        self.session = session
        self.table_name = table_name
        self.mytable = table

    def add_record(self, new_data: dict) -> None:
        """
        Add a new data to the table
        """
        try:
            self.session.execute(self.mytable.insert().values(**new_data))
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise ValueError("Failed to add new record: ", e)

    def add_multiple_records(self, new_data_list: list) -> None:
        """
        Add multiple records to the table
        """
        try:
            if len(new_data_list) == 0:
               return None
            self.session.execute(self.mytable.insert().values(new_data_list))
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise ValueError("Failed to add new records: ", e)

    def update_record(self, id, data: dict) -> None:
        """
        Update an existing record in the table
        """
        stmt = update(self.mytable).where(self.mytable.c.id == id).values(**data)
        result = self.session.execute(stmt)
        if result.rowcount == 0:
            raise ValueError(f"No record with id={id} found")
        self.session.commit()

    def delete_record(self, id) -> None:
        """
        Delete an existing record with id in the table
        """
        self.session.execute(self.mytable.delete().where(self.mytable.c.id == id))
        self.session.commit()

    def get_record_by_id(self, id) -> dict:
        """
        Retrieve a record in the table by ID.
        """
        record = self.session.query(self.mytable).filter_by(id=id).first()
        if record is None:
            return []
        return [{col.name: getattr(record, col.name)} for col in self.mytable.columns]

    def get_all_files(self) -> list:
        """ Get all file paths created in database

        Returns:
            list: list of paths
        """
        file_roots = self.session.query(self.mytable.c.file_root).all()
        file_names = self.session.query(self.mytable.c.file_name).all()

        res = []
        for idx in range(0, len(file_roots)):
            file_full_path = file_roots[idx][0] + "\\" + file_names[idx][0]
            res.append(file_full_path)

        return res

    def get_mytable_schema(self) -> dict:
        """
        Retrieve the schema of the table.
        """
        schema = {}
        for column in self.mytable.columns:
            schema[column.name] = {
                'type': str(column.type),
                'nullable': column.nullable,
                'default': column.default,
                'primary_key': column.primary_key
            }
        return schema
