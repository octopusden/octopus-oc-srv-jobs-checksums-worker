CREATE OR REPLACE PACKAGE BODY FOO
AS
    PROCEDURE BAR(COLL COLL_TYPE)
    IS
    BEGIN--comments added
        COLL.DELETE;
        COLL.DELETE("changeD");
        COLL.DELETE(42);/* another comment */
        COLL.DELETE(42, 64);
    END bar;
END FOO;
