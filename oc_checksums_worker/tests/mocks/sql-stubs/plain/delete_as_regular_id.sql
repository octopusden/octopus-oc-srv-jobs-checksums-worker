CREATE OR REPLACE PACKAGE BODY FOO
AS
    PROCEDURE BAR(COLL COLL_TYPE)
    IS
    BEGIN
        COLL.DELETE;
        COLL.DELETE();
        COLL.DELETE(42);
        COLL.DELETE(42, 64);
    END BAR;
END FOO;
